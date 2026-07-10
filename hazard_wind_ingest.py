#!/usr/bin/env python3
"""
StormAuditor HAZARD ENGINE — national wind layer ingester (v1).

Builds and stores, once per day for the whole CONUS, the per-cell wind data a
commercial suite computes: this is the "process nationally once, report
per-address instantly" architecture used by Benchmark-class products.

For each 06Z-06Z convective day it produces TWO independent gridded layers:

  ANL  — NWS 2.5 km analysis of record (URMA from AWS, byte-ranged GUST only;
         automatic RTMA fallback from Iowa State mtarchive for dates beyond
         AWS retention). 24 hourly fields -> per-cell daily max gust + hours
         at/above 40 and 58 mph.
  HRRR — NOAA 3 km convection-allowing model analysis (f00 GUST, byte-ranged
         from AWS, full multi-year archive). Same daily max + durations.
         Independent evidence; URMA can smooth narrow convective swaths that
         HRRR (radar-assimilating) resolves.

Cells with daily max >= FLOOR (40 mph) are stored as points, tagged by source,
via the locked hz_wind_ingest RPC. No regridding: each source keeps its native
grid and the fusion happens at query time in the report RPC.

Per-day cost: 48 byte-ranged GUST slices (~2-6 MB each). Runs free in GitHub
Actions. Backfill via hazard_backfill_walker.py.

Env: SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET
Optional: INGEST_DATE (YYYYMMDD, comma list, or a:b range; default yesterday
UTC), SOURCES (ANL,HRRR default both), FLOOR_MPH (default 40)

Deps: pygrib numpy requests
"""
import os, gzip, json, time, datetime as dt
import urllib.request
import numpy as np
import pygrib
import requests

UA = {"User-Agent": "StormAuditor-HazardEngine/1.0"}
UTC = dt.timezone.utc
MS2MPH = 2.2369363
FLOOR = float(os.environ.get("FLOOR_MPH", "40"))
DUR_T = (40.0, 58.0)

URMA = "https://noaa-urma-pds.s3.amazonaws.com"
HRRR = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com"
MT = "https://mtarchive.geol.iastate.edu"

_LATLON = {}   # per-source (lats, lons) cache


def _read(url, rng=None, timeout=150, retries=3):
    hdr = dict(UA)
    if rng:
        hdr["Range"] = rng
    last = None
    for a in range(retries):
        try:
            return urllib.request.urlopen(
                urllib.request.Request(url, headers=hdr), timeout=timeout).read()
        except Exception as e:
            last = e
            time.sleep(2 * (a + 1))
    raise RuntimeError(f"{url}: {last}")


def _grib_vals(blob, src):
    with open("/tmp/_hz.grib2", "wb") as fh:
        fh.write(blob)
    g = pygrib.open("/tmp/_hz.grib2")
    m = g[1]
    v = m.values.astype("float32")
    if src not in _LATLON:
        la, lo = m.latlons()
        lo = np.asarray(lo)
        lo = np.where(lo > 180, lo - 360.0, lo)
        _LATLON[src] = (np.asarray(la, dtype="float32"), lo.astype("float32"))
    g.close()
    return np.asarray(v)


def _idx_range(idx_txt, field):
    lines = idx_txt.splitlines()
    for i, line in enumerate(lines):
        f = line.split(":")
        if len(f) > 3 and f[3] == field:
            s = int(f[1])
            e = int(lines[i + 1].split(":")[1]) - 1 if i + 1 < len(lines) else ""
            return f"bytes={s}-{e}"
    return None


def gust_anl(t):
    """NWS analysis GUST (m/s): URMA byte-range, else RTMA whole-field."""
    ds, hh = t.strftime("%Y%m%d"), t.hour
    stem = f"urma2p5.{ds}/urma2p5.t{hh:02d}z.2dvaranl_ndfd.grb2_wexp"
    try:
        rng = _idx_range(_read(f"{URMA}/{stem}.idx", timeout=45, retries=1)
                         .decode(), "GUST")
        if rng:
            return _grib_vals(_read(f"{URMA}/{stem}", rng=rng), "ANL_URMA"), "ANL_URMA"
    except Exception:
        pass
    url = f"{MT}/{ds[:4]}/{ds[4:6]}/{ds[6:]}/grib2/ncep/RTMA/{ds}{hh:02d}00_GUST.grib2"
    try:
        return _grib_vals(_read(url), "ANL_RTMA"), "ANL_RTMA"
    except Exception:
        return None, None


def gust_hrrr(t):
    ds, hh = t.strftime("%Y%m%d"), t.hour
    stem = f"hrrr.{ds}/conus/hrrr.t{hh:02d}z.wrfsfcf00.grib2"
    try:
        rng = _idx_range(_read(f"{HRRR}/{stem}.idx", timeout=45, retries=2)
                         .decode(), "GUST")
        if rng:
            return _grib_vals(_read(f"{HRRR}/{stem}", rng=rng), "HRRR"), "HRRR"
    except Exception:
        pass
    return None, None


def daily_layer(date_str, fetch, tag):
    """06Z..05Z convective-day max (mph) + duration hour counts per cell."""
    d0 = dt.datetime.strptime(date_str, "%Y%m%d").replace(tzinfo=UTC)
    hours = [d0 + dt.timedelta(hours=h) for h in range(6, 30)]
    dmax = None; d40 = None; d58 = None; used = 0; srcs = set()
    for t in hours:
        v, src = fetch(t)
        if v is None:
            continue
        mph = v * MS2MPH
        if dmax is None:
            dmax = mph
            d40 = (mph >= DUR_T[0]).astype("int16")
            d58 = (mph >= DUR_T[1]).astype("int16")
        elif mph.shape == dmax.shape:
            np.fmax(dmax, mph, out=dmax)
            d40 += (mph >= DUR_T[0]).astype("int16")
            d58 += (mph >= DUR_T[1]).astype("int16")
        else:   # grid changed mid-day (URMA<->RTMA boundary); keep first grid
            continue
        used += 1; srcs.add(src)
    if dmax is None:
        return None
    src_key = sorted(srcs)[0].split("_")[0] if tag == "HRRR" else "ANL"
    lats, lons = _LATLON[sorted(srcs)[0]]
    ys, xs = np.where(dmax >= FLOOR)
    pts = [{"lon": round(float(lons[y, x]), 3), "lat": round(float(lats[y, x]), 3),
            "v": int(round(float(dmax[y, x]))),
            "d40": int(d40[y, x]), "d58": int(d58[y, x])}
           for y, x in zip(ys.tolist(), xs.tolist())]
    return {"src": src_key, "detail": "/".join(sorted(srcs)),
            "hours": used, "points": pts}


def rpc(base, anon, name, payload, timeout=180):
    last = ""
    for a in range(4):
        try:
            r = requests.post(f"{base}/rest/v1/rpc/{name}",
                              headers={"apikey": anon,
                                       "Authorization": f"Bearer {anon}",
                                       "Content-Type": "application/json"},
                              data=json.dumps(payload), timeout=timeout)
            if r.status_code < 300:
                return r
            last = f"{name} {r.status_code}: {r.text[:200]}"
        except Exception as e:
            last = f"{name} exception: {e}"
        time.sleep(1.5 * (a + 1))
    raise RuntimeError(last)


def process_day(date_str, base, anon, secret, sources=("ANL", "HRRR")):
    iso = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    total = 0
    for tag, fetch in (("ANL", gust_anl), ("HRRR", gust_hrrr)):
        if tag not in sources:
            continue
        lay = daily_layer(date_str, fetch, tag)
        if not lay:
            print(f"  {iso} {tag}: no data")
            continue
        pts = lay["points"]
        for i in range(0, max(len(pts), 1), 4000):
            rpc(base, anon, "hz_wind_ingest",
                {"p_secret": secret, "p_date": iso, "p_src": lay["src"],
                 "p_detail": lay["detail"], "p_hours": lay["hours"],
                 "p_points": pts[i:i + 4000], "p_append": i > 0})
        total += len(pts)
        print(f"  {iso} {tag}: {len(pts)} cells >= {FLOOR:.0f} mph "
              f"({lay['detail']}, {lay['hours']}/24 hrs)")
    return total


def main():
    raw = os.environ.get("INGEST_DATE") or \
        (dt.datetime.now(UTC).date() - dt.timedelta(days=1)).strftime("%Y%m%d")
    dates = []
    for tok in [d.strip() for d in raw.split(",") if d.strip()]:
        if ":" in tok:
            a, b = tok.split(":")
            cur = dt.datetime.strptime(a, "%Y%m%d").date()
            d1 = dt.datetime.strptime(b, "%Y%m%d").date()
            while cur <= d1:
                dates.append(cur.strftime("%Y%m%d")); cur += dt.timedelta(days=1)
        else:
            dates.append(tok)
    sources = tuple((os.environ.get("SOURCES") or "ANL,HRRR").split(","))
    base = os.environ["SUPABASE_URL"].rstrip("/")
    anon = os.environ["SUPABASE_ANON_KEY"]
    secret = os.environ["INGEST_SECRET"]
    print(f"Hazard wind ingest: {len(dates)} day(s), sources {sources}")
    for d in dates:
        process_day(d, base, anon, secret, sources)


if __name__ == "__main__":
    main()
