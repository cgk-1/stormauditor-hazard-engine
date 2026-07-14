#!/usr/bin/env python3
"""
StormAuditor Hazard Engine v2 — HRRR wind layer ingester (local-clock days).

This is the ONLY grid this engine downloads. The analysis-of-record wind layer
and the MESH hail layer come from your existing Wind/Hail Explorer tables —
nothing is downloaded twice.

DAY CONVENTION: identical to the Explorer v3 pipelines — each "day" is the
LOCAL CALENDAR DAY (midnight to midnight, DST-aware) of the state's dominant
timezone. States are grouped by timezone; each UTC hour field is downloaded
once and shared across groups (an internal cache), so a full CONUS local day
costs ~28 byte-ranged GUST slices (~2-4 MB each) total.

Per local day and state it stores every HRRR 3-km cell with daily max gust
>= 40 mph (v, plus hours >= 40 and >= 58 at that cell), and the HRRR daily
max sampled at every ASOS/AWOS station (hz_station_bg, src='HRRR') so
objective-analysis innovations are exact.

Env: SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET
Optional: INGEST_DATE (local YYYYMMDD, single/comma/range a:b; default
          yesterday), STATES, FLOOR_MPH (default 40)
Deps: pygrib numpy shapely requests
"""
import os, json, time, datetime as dt
from zoneinfo import ZoneInfo
import urllib.request
import numpy as np
import pygrib
import requests
from shapely.geometry import shape, Point
from shapely.prepared import prep

UA = {"User-Agent": "StormAuditor-HazardEngine/2.0"}
UTC = dt.timezone.utc
MS2MPH = 2.2369363
FLOOR = float(os.environ.get("FLOOR_MPH", "30"))
HRRR = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com"

STATE_TZ = {
 "Alabama":"America/Chicago","Arizona":"America/Phoenix","Arkansas":"America/Chicago",
 "California":"America/Los_Angeles","Colorado":"America/Denver","Connecticut":"America/New_York",
 "Delaware":"America/New_York","Florida":"America/New_York","Georgia":"America/New_York",
 "Idaho":"America/Boise","Illinois":"America/Chicago","Indiana":"America/Indiana/Indianapolis",
 "Iowa":"America/Chicago","Kansas":"America/Chicago","Kentucky":"America/New_York",
 "Louisiana":"America/Chicago","Maine":"America/New_York","Maryland":"America/New_York",
 "Massachusetts":"America/New_York","Michigan":"America/Detroit","Minnesota":"America/Chicago",
 "Mississippi":"America/Chicago","Missouri":"America/Chicago","Montana":"America/Denver",
 "Nebraska":"America/Chicago","Nevada":"America/Los_Angeles","New Hampshire":"America/New_York",
 "New Jersey":"America/New_York","New Mexico":"America/Denver","New York":"America/New_York",
 "North Carolina":"America/New_York","North Dakota":"America/Chicago","Ohio":"America/New_York",
 "Oklahoma":"America/Chicago","Oregon":"America/Los_Angeles","Pennsylvania":"America/New_York",
 "Rhode Island":"America/New_York","South Carolina":"America/New_York","South Dakota":"America/Chicago",
 "Tennessee":"America/Chicago","Texas":"America/Chicago","Utah":"America/Denver",
 "Vermont":"America/New_York","Virginia":"America/New_York","Washington":"America/Los_Angeles",
 "West Virginia":"America/New_York","Wisconsin":"America/Chicago","Wyoming":"America/Denver",
}
PERMITTED_STATES = set(STATE_TZ)
NAME2ABBR = {"Alabama":"AL","Arizona":"AZ","Arkansas":"AR","California":"CA",
 "Colorado":"CO","Connecticut":"CT","Delaware":"DE","Florida":"FL","Georgia":"GA",
 "Idaho":"ID","Illinois":"IL","Indiana":"IN","Iowa":"IA","Kansas":"KS",
 "Kentucky":"KY","Louisiana":"LA","Maine":"ME","Maryland":"MD","Massachusetts":"MA",
 "Michigan":"MI","Minnesota":"MN","Mississippi":"MS","Missouri":"MO","Montana":"MT",
 "Nebraska":"NE","Nevada":"NV","New Hampshire":"NH","New Jersey":"NJ",
 "New Mexico":"NM","New York":"NY","North Carolina":"NC","North Dakota":"ND",
 "Ohio":"OH","Oklahoma":"OK","Oregon":"OR","Pennsylvania":"PA","Rhode Island":"RI",
 "South Carolina":"SC","South Dakota":"SD","Tennessee":"TN","Texas":"TX",
 "Utah":"UT","Vermont":"VT","Virginia":"VA","Washington":"WA",
 "West Virginia":"WV","Wisconsin":"WI","Wyoming":"WY"}

# Census cartographic boundaries primary (complete geography incl. Keys);
# PublicaMundi 52-feature GeoJSON fallback. Same policy as Explorer v3.
CENSUS_URL = ("https://raw.githubusercontent.com/uscensusbureau/citysdk/master/"
              "v2/GeoJSON/500k/2019/state.json")
CENSUS_URL2 = ("https://www2.census.gov/geo/tiger/GENZ2023/shp/"
               "cb_2023_us_state_500k.zip")
FALLBACK_URL = ("https://raw.githubusercontent.com/PublicaMundi/MappingAPI/"
                "master/data/geojson/us-states.json")
_GEOM = {}
_ALL = None


def load_state_geom(name):
    """Prepared geometry for a state (Census 500k primary, PublicaMundi
    fallback), cached."""
    global _ALL
    if name in _GEOM:
        return _GEOM[name]
    if _ALL is None:
        _ALL = {}
        for url in (CENSUS_URL, FALLBACK_URL):
            try:
                gj = json.loads(urllib.request.urlopen(
                    urllib.request.Request(url, headers=UA), timeout=90).read())
                for f in gj["features"]:
                    nm = f["properties"].get("NAME") or f["properties"].get("name")
                    if nm and nm not in _ALL:
                        _ALL[nm] = f["geometry"]
                if len(_ALL) >= 48:
                    break
            except Exception as e:
                print(f"  [warn] boundary source {url.split('/')[2]}: {e}")
    if name not in _ALL:
        raise RuntimeError(f"no boundary for {name}")
    g = shape(_ALL[name]).buffer(0)
    _GEOM[name] = (g, prep(g))
    return _GEOM[name]


_HOUR_CACHE = {}   # utc iso-hour -> np.array (m/s) | None
_LATLON = None


def _fetch_field(stem, field, want_max_lvl):
    """Byte-range one GRIB message; returns m/s array or None."""
    global _LATLON
    try:
        idx = urllib.request.urlopen(
            urllib.request.Request(f"{HRRR}/{stem}.idx", headers=UA),
            timeout=45).read().decode().splitlines()
        s = e = None
        for i, line in enumerate(idx):
            f = line.split(":")
            if len(f) > 4 and f[3] == field and want_max_lvl in line:
                s = int(f[1])
                e = int(idx[i+1].split(":")[1]) - 1 if i+1 < len(idx) else ""
                break
        if s is None:
            return None
        blob = urllib.request.urlopen(urllib.request.Request(
            f"{HRRR}/{stem}", headers={**UA, "Range": f"bytes={s}-{e}"}),
            timeout=120).read()
        with open("/tmp/_h.grib2", "wb") as fh:
            fh.write(blob)
        g = pygrib.open("/tmp/_h.grib2")
        m = g[1]
        arr = np.asarray(m.values, dtype="float32")
        if _LATLON is None:
            la, lo = m.latlons()
            lo = np.asarray(lo)
            _LATLON = (np.asarray(la, dtype="float32"),
                       np.where(lo > 180, lo - 360.0, lo).astype("float32"))
        g.close()
        return arr
    except Exception:
        return None


def hrrr_hour(t):
    """HRRR near-surface wind proxy for the hour ENDING at t (m/s):
    elementwise max of the f01 hourly-maximum 10 m wind (the Kain et al.
    2010 hourly-maximum-field diagnostic, which captures sub-hourly
    convective peaks the instantaneous analysis misses) and the f01 GUST.
    Taking the max is a floor relationship (gust >= sustained maximum), so
    no conversion coefficient is introduced. Cached across tz groups."""
    key = t.strftime("%Y%m%d%H")
    if key in _HOUR_CACHE:
        return _HOUR_CACHE[key]
    init = t - dt.timedelta(hours=1)          # f01 valid at t
    ds, hh = init.strftime("%Y%m%d"), init.hour
    stem = f"hrrr.{ds}/conus/hrrr.t{hh:02d}z.wrfsfcf01.grib2"
    arr = None
    for attempt in range(3):
        wm = _fetch_field(stem, "WIND", "0-1 hour max")
        gu = _fetch_field(stem, "GUST", "1 hour fcst")
        if wm is not None or gu is not None:
            if wm is None:
                arr = gu
            elif gu is None or gu.shape != wm.shape:
                arr = wm
            else:
                arr = np.fmax(wm, gu)
            break
        time.sleep(2 * (attempt + 1))
    if arr is None:   # last resort: f00 instantaneous gust
        stem0 = f"hrrr.{t.strftime('%Y%m%d')}/conus/hrrr.t{t.hour:02d}z.wrfsfcf00.grib2"
        arr = _fetch_field(stem0, "GUST", "anl")
    _HOUR_CACHE[key] = arr
    if len(_HOUR_CACHE) > 40:
        for k in sorted(_HOUR_CACHE)[:8]:
            _HOUR_CACHE.pop(k, None)
    return arr


def group_daily_max(tzname, local_date_str):
    """Daily max (mph) + d40/d58 hour counts over the LOCAL day for one tz."""
    tz = ZoneInfo(tzname)
    y, m, d = int(local_date_str[:4]), int(local_date_str[4:6]), int(local_date_str[6:])
    d0 = dt.datetime(y, m, d, tzinfo=tz)
    hours = [(d0 + dt.timedelta(hours=h)).astimezone(UTC).replace(
                minute=0, second=0, microsecond=0) for h in range(24)]
    dmax = d40 = d58 = None
    used = 0
    for t in hours:
        v = hrrr_hour(t)
        if v is None:
            continue
        mph = v * MS2MPH
        if dmax is None:
            dmax = mph.copy()
            d40 = (mph >= 40).astype("int16")
            d58 = (mph >= 58).astype("int16")
        else:
            np.fmax(dmax, mph, out=dmax)
            d40 += (mph >= 40).astype("int16")
            d58 += (mph >= 58).astype("int16")
        used += 1
    return dmax, d40, d58, used


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
        except Exception as ex:
            last = f"{name} exception: {ex}"
        time.sleep(1.5 * (a + 1))
    raise RuntimeError(last)


_STATIONS = None
def load_stations(base, anon, secret):
    global _STATIONS
    if _STATIONS is None:
        try:
            r = rpc(base, anon, "hz_stations_fetch", {"p_secret": secret}, 60)
            _STATIONS = r.json() if r.text and r.text != "null" else []
        except Exception as e:
            print(f"  [warn] hz_stations_fetch unavailable ({e}); "
                  f"skipping station backgrounds this run")
            _STATIONS = []
    return _STATIONS


def process_local_date(local_date, states, base, anon, secret):
    date_iso = f"{local_date[:4]}-{local_date[4:6]}-{local_date[6:]}"
    groups = {}
    for st in states:
        if st in PERMITTED_STATES:
            groups.setdefault(STATE_TZ[st], []).append(st)
    stations = load_stations(base, anon, secret)
    la = lo = None
    stored = 0

    for tzname, group_states in sorted(groups.items()):
        dmax, d40, d58, used = group_daily_max(tzname, local_date)
        if dmax is None:
            print(f"  {date_iso} [{tzname}]: no HRRR data")
            continue
        la, lo = _LATLON
        # v2.6: uncapped coarse field samples (every 8th cell, >=5 mph)
        cg_v = dmax[::8, ::8]; cg_la = la[::8, ::8]; cg_lo = lo[::8, ::8]
        cyy, cxx = np.where(cg_v >= 5)
        cg_lonv = cg_lo[cyy, cxx]; cg_latv = cg_la[cyy, cxx]
        cg_vv = cg_v[cyy, cxx]
        ys, xs = np.where(dmax >= FLOOR)
        # vectorized candidate arrays (per-state bbox filtering in numpy,
        # shapely only on the survivors -- ~50x faster than the python loop)
        c_lon = lo[ys, xs]; c_lat = la[ys, xs]
        c_v = dmax[ys, xs]; c_d40 = d40[ys, xs]; c_d58 = d58[ys, xs]
        # station backgrounds for stations whose state tz is this group
        bg_rows = []
        gset = {NAME2ABBR[s] for s in group_states}
        for stn in stations:
            if stn.get("state") not in gset:
                continue
            j = int(np.argmin((la - stn["lat"])**2 + (lo - stn["lon"])**2))
            yy, xx = np.unravel_index(j, la.shape)
            bg_rows.append({"stid": stn["stid"],
                            "bg": int(round(float(dmax[yy, xx])))})
        for i in range(0, max(len(bg_rows), 1), 3000):
            if bg_rows:
                rpc(base, anon, "hz_station_bg_ingest",
                    {"p_secret": secret, "p_date": date_iso, "p_src": "HRRR",
                     "p_rows": bg_rows[i:i+3000], "p_append": i > 0})

        for st in group_states:
            try:
                geom, pg = load_state_geom(st)
                minx, miny, maxx, maxy = geom.bounds
                m = ((c_lon >= minx) & (c_lon <= maxx) &
                     (c_lat >= miny) & (c_lat <= maxy))
                pts = []
                for i in np.where(m)[0]:
                    x, y = float(c_lon[i]), float(c_lat[i])
                    if pg.contains(Point(x, y)):
                        pts.append({"lon": round(x, 3), "lat": round(y, 3),
                                    "v": int(round(float(c_v[i]))),
                                    "d40": int(c_d40[i]),
                                    "d58": int(c_d58[i])})
                # coarse field samples for this state (uncapped)
                cm = ((cg_lonv >= minx) & (cg_lonv <= maxx) &
                      (cg_latv >= miny) & (cg_latv <= maxy))
                cpts = [{"lon": round(float(cg_lonv[i]), 2),
                         "lat": round(float(cg_latv[i]), 2),
                         "v": int(round(float(cg_vv[i])))}
                        for i in np.where(cm)[0]
                        if pg.contains(Point(float(cg_lonv[i]),
                                             float(cg_latv[i])))]
                for i in range(0, len(cpts), 4000):
                    rpc(base, anon, "hz_bg_coarse_ingest",
                        {"p_secret": secret, "p_date": date_iso,
                         "p_src": "HRRR", "p_points": cpts[i:i+4000]})
                if not pts:
                    continue
                for i in range(0, len(pts), 4000):
                    rpc(base, anon, "hz_hrrr_ingest",
                        {"p_secret": secret, "p_state": st, "p_date": date_iso,
                         "p_hours": used, "p_points": pts[i:i+4000],
                         "p_append": i > 0})
                stored += 1
                print(f"  {date_iso}  {st:16s} {len(pts)} HRRR cells >= "
                      f"{FLOOR:.0f} mph ({used}/24 hrs)")
            except Exception as ex:
                print(f"  [error] {date_iso} {st}: {ex}")
    if stored == 0:
        print(f"{date_iso}: no HRRR wind >= {FLOOR:.0f} mph on land.")
    return stored


def main():
    raw = os.environ.get("INGEST_DATE") or \
        (dt.datetime.now(UTC).date() - dt.timedelta(days=1)).strftime("%Y%m%d")
    dates = []
    for tok in [t.strip() for t in raw.split(",") if t.strip()]:
        if ":" in tok:
            a, b = tok.split(":")
            cur = dt.datetime.strptime(a, "%Y%m%d").date()
            d1 = dt.datetime.strptime(b, "%Y%m%d").date()
            while cur <= d1:
                dates.append(cur.strftime("%Y%m%d")); cur += dt.timedelta(days=1)
        else:
            dates.append(tok)
    base = os.environ["SUPABASE_URL"].rstrip("/")
    anon = os.environ["SUPABASE_ANON_KEY"]
    secret = os.environ["INGEST_SECRET"]
    states_env = os.environ.get("STATES")
    states = ([s.strip() for s in states_env.split(",")] if states_env
              else sorted(PERMITTED_STATES))
    print(f"HRRR ingest v2 (local-clock days): {len(dates)} date(s), "
          f"{len(states)} state(s)")
    for d in dates:
        process_local_date(d, states, base, anon, secret)


if __name__ == "__main__":
    main()
