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
>= FLOOR_MPH (v, plus hours >= 40 and >= 58 at that cell), and the HRRR daily
max sampled at every ASOS/AWOS station (hz_station_bg, src='HRRR') so
objective-analysis innovations are exact.

STAGE 1 HARDENING (Archive Phase 5, 2026-10-07) - the math is unchanged; the
payloads are byte-identical to the previous version on normal days:
  * Every hour needs BOTH f01 fields (WIND 10 m 0-1 h max and GUST 1 h fcst).
    The old code silently fell back to one field, or to the f00 analysis
    gust, or skipped the hour. Now a missing/invalid hour fails its tz group
    (those states are NOT written, quarantine record, failed run) - except
    the hours listed in HRRR_F01_FALLBACK_OK, which are explicitly allowed to
    use the f00 GUST analysis if their f01 file is ever absent (logged and
    counted in the completeness JSON).
  * Every message is validated: discipline/category/parameter, the pinned
    section-3 grid (Lambert 1799x1059, md5 78367561...), valid time, no
    bitmap, finite values 0-150 m/s, lat/lon bounds; each state's payload is
    range-checked.
  * Station list / per-state / station-background failures are no longer
    swallowed; complete states are still written; the job exits non-zero.
  * KNOWN, KEPT FOR PARITY (owner-approved v4 fix pending, plan T7): every tz
    group's first hz_station_bg_ingest chunk uses p_append=false, which
    deletes the whole date, so only the last group (Arizona) survives.
  * State boundaries (Census 500k 2019) are vendored and md5-checked.
  * DRY_RUN=1, DATE=YYYY-MM-DD|START..END, completeness summary and the
    FEED_RESULT json line: see feedguard.py.

DAY_CONVENTION=v4 (Archive Phase 5 Stage 3, 2026-10-07; default v3 = unchanged):
  * T1: every HRRR cell uses its OWN zone's local day (tzwin.py,
    data/tz/tz_hrrr.npz); the zone windows are hours the state groups already
    fetch (one shared hour cache); each cell takes its own window's max and
    d40/d58 counts. hz_bg_coarse HRRR samples come from the same composite.
  * T3: the window has the real number of local hours (23 on spring-forward,
    25 on fall-back days) instead of always 24. p_hours = the state's own
    zone window length.
  * T7: hz_station_bg HRRR is written ONCE per date for every station (first
    chunk deletes the date, the rest append) from the own-zone composite. v3
    kept only the last tz group (Arizona).
  * Hour alignment is kept as v3: the HRRR day is the 24 (23/25) hourly fields
    ENDING at local 00:00 .. 23:00 (hrrr_hour(t) = hour ending t), i.e. local
    23:00 D-1 .. 23:00 D. V4_HRRR_HOUR_ENDING=1 (dry runs only, owner decision
    pending, report item T12) uses the hours ending 01:00 .. 24:00 instead.
  * Test flags (DRY_RUN only): V4_ZONES=state, V4_DST=0 (see tzwin.py).

Env: SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET
Optional: DATE / INGEST_DATE (local dates; default yesterday), STATES,
          FLOOR_MPH (default 30; the workflows pass 35), DRY_RUN, FEED_OUT_DIR,
          HZ_STATIONS_FILE (offline station list for dry runs),
          DAY_CONVENTION (v3|v4), V4_ZONES / V4_DST / V4_HRRR_HOUR_ENDING (dry runs)
Deps: requirements.txt (exact pins)
"""
import os, json, gzip, time, struct, hashlib, tempfile, datetime as dt
from zoneinfo import ZoneInfo
import numpy as np
import pygrib
from shapely.geometry import shape, Point
from shapely.prepared import prep

import feedguard as fg
import tzwin

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

# Census cartographic boundaries 500k 2019 (complete geography incl. Keys),
# vendored 2026-10-07 from raw.githubusercontent.com/uscensusbureau/citysdk/
# master/v2/GeoJSON/500k/2019/state.json (gzip; md5 of the JSON enforced).
# Every earlier run used this exact file (the PublicaMundi fallback was only
# consulted when Census was unreachable).
HERE = os.path.dirname(os.path.abspath(__file__))
BOUNDARY_FILE = os.path.join(HERE, "data", "census-2019-500k-state.json.gz")
BOUNDARY_MD5 = "00c7e08c870d3dca1a46a8cb97233b02"
_GEOM = {}
_ALL = None

# HRRR wrfsfc message contract (identical 2021-10 -> 2026-10).
HRRR_SEC3_MD5 = "78367561440d7c7b608b8532a02e4780"   # Lambert 1799x1059, 3 km
HRRR_SHAPE = (1059, 1799)
HRRR_LAT = (21.138123, 52.615653)
HRRR_LON = (-134.095480, -60.917193)
HRRR_MAX_MS = fg.env_float("HRRR_MAX_MS", 150.0)
FIELD_IDS = {("WIND", "0-1 hour max"): (0, 2, 1), ("GUST", "1 hour fcst"): (0, 2, 22),
             ("GUST", "anl"): (0, 2, 22)}
# f01 init hours (YYYYMMDDHH) that may use the f00 GUST analysis for the hour
# they end, IF their f01 file is ever absent. Both were flagged by a 2026-10-07
# availability probe (non-200) and re-verified PRESENT the same day (file + idx
# + both fields) - so today they ingest normally. Any OTHER missing hour fails.
HRRR_F01_FALLBACK_OK = {"2023060817", "2023081523"}


def load_state_geom(name):
    """Prepared geometry for a state (vendored Census 500k), cached."""
    global _ALL
    if name in _GEOM:
        return _GEOM[name]
    if _ALL is None:
        with open(BOUNDARY_FILE, "rb") as fh:
            raw = gzip.decompress(fh.read())
        got = hashlib.md5(raw).hexdigest()
        if got != BOUNDARY_MD5:
            raise fg.ValidationError(f"state boundary file md5 {got} != pinned {BOUNDARY_MD5}")
        gj = json.loads(raw)
        _ALL = {}
        for f in gj["features"]:
            nm = f["properties"].get("NAME") or f["properties"].get("name")
            if nm and nm not in _ALL:
                _ALL[nm] = f["geometry"]
    if name not in _ALL:
        raise RuntimeError(f"no boundary for {name}")
    g = shape(_ALL[name]).buffer(0)
    _GEOM[name] = (g, prep(g))
    return _GEOM[name]


_HOUR_CACHE = {}   # utc iso-hour -> np.array (m/s) | Exception
_LATLON = None
HOUR_NOTES = {}    # utc hour key -> note (e.g. f00 fallback)


def _sec3_md5(data):
    p = 16
    while p + 5 <= len(data):
        L = struct.unpack(">I", data[p:p + 4])[0]
        if L < 5:
            break
        if data[p + 4] == 3:
            return hashlib.md5(data[p:p + L]).hexdigest()
        p += L
    return None


def _fetch_field(stem, field, want_max_lvl, valid):
    """Byte-range one GRIB message; returns the validated m/s array.
    UpstreamMissing when the file/idx/field is not published."""
    global _LATLON
    label = f"HRRR {stem.split('/')[0][5:]} {stem.split('.')[-2]} {field} {want_max_lvl}"
    idx = fg.http_get(f"{HRRR}/{stem}.idx", headers=UA, timeout=45, retries=4,
                      what=f"{label} idx").decode().splitlines()
    hits = []
    for i, line in enumerate(idx):
        f = line.split(":")
        if len(f) > 4 and f[3] == field and want_max_lvl in line:
            hits.append(i)
    if not hits:
        raise fg.UpstreamMissing(f"{label}: field not in the .idx")
    if len(hits) != 1:
        raise fg.ValidationError(f"{label}: {len(hits)} matching .idx lines (expected 1)")
    i = hits[0]
    s = int(idx[i].split(":")[1])
    e = int(idx[i+1].split(":")[1]) - 1 if i+1 < len(idx) else None
    blob = fg.http_get(f"{HRRR}/{stem}", headers=UA, byte_range=(s, e), timeout=120, retries=4,
                       what=f"{label} range")
    if blob[:4] != b"GRIB" or blob[-4:] != b"7777" or blob[7] != 2:
        raise fg.ValidationError(f"{label}: not one complete GRIB2 message")
    s3 = _sec3_md5(blob)
    if s3 != HRRR_SEC3_MD5:
        raise fg.ValidationError(f"{label}: grid definition changed (section 3 md5 {s3})")
    fd, path = tempfile.mkstemp(suffix=".grib2")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(blob)
        g = pygrib.open(path)
        try:
            if g.messages != 1:
                raise fg.ValidationError(f"{label}: {g.messages} messages in the slice")
            m = g[1]
            got = (m["discipline"], m["parameterCategory"], m["parameterNumber"])
            if got != FIELD_IDS[(field, want_max_lvl)]:
                raise fg.ValidationError(f"{label}: parameter {got} != {FIELD_IDS[(field, want_max_lvl)]}")
            vt = (int(m["validityDate"]), int(m["validityTime"]))
            if vt != (int(valid.strftime("%Y%m%d")), valid.hour * 100):
                raise fg.ValidationError(f"{label}: valid time {vt} != {valid.isoformat()}")
            raw_vals = m.values
            if np.ma.isMaskedArray(raw_vals) and np.ma.is_masked(raw_vals):
                raise fg.ValidationError(f"{label}: field has masked points")
            arr = np.asarray(raw_vals, dtype="float32")
            if _LATLON is None:
                la, lo = m.latlons()
                lo = np.asarray(lo)
                lo = np.where(lo > 180, lo - 360.0, lo)
                if (la.shape != HRRR_SHAPE or abs(la.min() - HRRR_LAT[0]) > 1e-3
                        or abs(la.max() - HRRR_LAT[1]) > 1e-3 or abs(lo.min() - HRRR_LON[0]) > 1e-3
                        or abs(lo.max() - HRRR_LON[1]) > 1e-3):
                    raise fg.ValidationError(f"{label}: lat/lon bounds are not the HRRR grid")
                _LATLON = (np.asarray(la, dtype="float32"), lo.astype("float32"))
        finally:
            g.close()
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
    if arr.shape != HRRR_SHAPE:
        raise fg.ValidationError(f"{label}: shape {arr.shape}")
    if not np.isfinite(arr).all():
        raise fg.ValidationError(f"{label}: {int((~np.isfinite(arr)).sum())} non-finite values")
    if float(arr.min()) < 0 or float(arr.max()) > HRRR_MAX_MS:
        raise fg.ValidationError(f"{label}: range {arr.min()}..{arr.max()} m/s outside 0..{HRRR_MAX_MS}")
    return arr


def hrrr_hour(t):
    """HRRR near-surface wind proxy for the hour ENDING at t (m/s):
    elementwise max of the f01 hourly-maximum 10 m wind (the Kain et al.
    2010 hourly-maximum-field diagnostic, which captures sub-hourly
    convective peaks the instantaneous analysis misses) and the f01 GUST.
    Taking the max is a floor relationship (gust >= sustained maximum), so
    no conversion coefficient is introduced. Cached across tz groups.
    Raises on a missing/invalid hour (see HRRR_F01_FALLBACK_OK)."""
    key = t.strftime("%Y%m%d%H")
    if key in _HOUR_CACHE:
        v = _HOUR_CACHE[key]
        if isinstance(v, Exception):
            raise v
        return v
    init = t - dt.timedelta(hours=1)          # f01 valid at t
    ds, hh = init.strftime("%Y%m%d"), init.hour
    stem = f"hrrr.{ds}/conus/hrrr.t{hh:02d}z.wrfsfcf01.grib2"
    try:
        try:
            wm = _fetch_field(stem, "WIND", "0-1 hour max", t)
            gu = _fetch_field(stem, "GUST", "1 hour fcst", t)
            if gu.shape != wm.shape:
                raise fg.ValidationError(f"HRRR {key}: WIND/GUST shapes differ")
            arr = np.fmax(wm, gu)
        except fg.UpstreamMissing as e:
            if init.strftime("%Y%m%d%H") not in HRRR_F01_FALLBACK_OK:
                raise
            stem0 = f"hrrr.{t.strftime('%Y%m%d')}/conus/hrrr.t{t.hour:02d}z.wrfsfcf00.grib2"
            arr = _fetch_field(stem0, "GUST", "anl", t)
            HOUR_NOTES[key] = f"hour ending {key}Z: f01 absent ({e}); used the f00 GUST analysis (allow-listed)"
            print(f"  [known gap] {HOUR_NOTES[key]}")
    except fg.FeedError as e:
        _HOUR_CACHE[key] = e
        raise
    _HOUR_CACHE[key] = arr
    if len(_HOUR_CACHE) > 40:
        for k in sorted(_HOUR_CACHE)[:8]:
            _HOUR_CACHE.pop(k, None)
    return arr


def local_hours(tzname, local_date_str):
    tz = ZoneInfo(tzname)
    y, m, d = int(local_date_str[:4]), int(local_date_str[4:6]), int(local_date_str[6:])
    d0 = dt.datetime(y, m, d, tzinfo=tz)
    return [(d0 + dt.timedelta(hours=h)).astimezone(UTC).replace(
            minute=0, second=0, microsecond=0) for h in range(24)]


HRRR_PUBLISH_GRACE_H = fg.env_float("HRRR_PUBLISH_GRACE_H", 12.0)


def preflight(run, key, local_date, states, policy, need=None):
    """Every hour the day needs (24 per tz group) must have its f01 .idx
    published before anything is written. Scheduled runs (policy defer)
    defer the day while recent hours are missing; the next dispatch re-runs
    yesterday anyway (daily-new always ingests yesterday). In practice the
    last needed file (init 06Z, or 07Z in winter) lands ~1 h after its init,
    long before the 10:10Z dispatch."""
    if need is None:
        need = sorted({t for tz in {STATE_TZ[s] for s in states} for t in local_hours(tz, local_date)})
    missing = []
    for t in need:
        init = t - dt.timedelta(hours=1)
        if init.strftime("%Y%m%d%H") in HRRR_F01_FALLBACK_OK:
            continue
        url = f"{HRRR}/hrrr.{init:%Y%m%d}/conus/hrrr.t{init.hour:02d}z.wrfsfcf01.grib2.idx"
        try:
            fg.http_head(url, headers=UA, timeout=30, retries=4, what=f"HRRR f01 idx {init:%Y%m%d %H}Z")
        except fg.UpstreamMissing:
            missing.append(t)
    run.set_received(key, "hours_needed", len(need))
    run.set_received(key, "hours_published", len(need) - len(missing))
    if not missing:
        return True
    lst = [t.strftime("%m-%d %HZ") for t in missing]
    if policy == "defer" and all(dt.datetime.now(UTC) - t < dt.timedelta(hours=HRRR_PUBLISH_GRACE_H)
                                 for t in missing):
        run.defer(key, f"{len(missing)} HRRR hour(s) not published yet {lst}; nothing written - the "
                       f"next dispatch re-runs yesterday", pending=lst)
        return False
    run.error(key, "HRRR hours", f"{len(missing)} needed HRRR hour(s) missing {lst}; day NOT written")
    return False


def group_daily_max(tzname, local_date_str):
    """Daily max (mph) + d40/d58 hour counts over the LOCAL day for one tz.
    Every one of the 24 hours must be present (raises otherwise)."""
    hours = local_hours(tzname, local_date_str)
    dmax = d40 = d58 = None
    used = 0
    for t in hours:
        v = hrrr_hour(t)
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


_STATIONS = None
def load_stations(run):
    global _STATIONS
    if _STATIONS is None:
        path = os.environ.get("HZ_STATIONS_FILE")
        if path:
            with open(path) as fh:
                st = json.load(fh)
        else:
            r = run.rpc_read("hz_stations_fetch", {"p_secret": run.secret}, 60)
            st = r.json() if r.text and r.text != "null" else []
        if not isinstance(st, list) or len(st) < 1000:
            raise fg.ValidationError(f"hz_stations_fetch returned "
                                     f"{len(st) if isinstance(st, list) else '?'} stations (expected > 1000)")
        _STATIONS = st
    return _STATIONS


def validate_state(st, geom, pts, cpts, used):
    minx, miny, maxx, maxy = geom.bounds
    seen = set()
    for p in pts:
        if not (minx <= p["lon"] <= maxx and miny <= p["lat"] <= maxy):
            raise fg.ValidationError(f"{st}: cell {p['lon']},{p['lat']} outside the state bounds")
        if not (FLOOR - 0.5 <= p["v"] <= 300):
            raise fg.ValidationError(f"{st}: cell value {p['v']} mph outside {FLOOR:.0f}-300")
        if not (0 <= p["d58"] <= p["d40"] <= used):
            raise fg.ValidationError(f"{st}: duration counts d40={p['d40']} d58={p['d58']} (hours {used})")
        k = (float(np.float32(p["lon"])), float(np.float32(p["lat"])))
        if k in seen:   # the table key is (state, date, lon::real, lat::real); a
            raise fg.ValidationError(f"{st}: duplicate cell {k} would be dropped by the DB")
        seen.add(k)
    for p in cpts:
        if not (5 <= p["v"] <= 300):
            raise fg.ValidationError(f"{st}: coarse value {p['v']} outside 5-300 mph")


def process_local_date(run, local_date, states, policy="strict"):
    date_iso = f"{local_date[:4]}-{local_date[4:6]}-{local_date[6:]}"
    key = date_iso
    groups = {}
    for st in states:
        groups.setdefault(STATE_TZ[st], []).append(st)
    run.expect(key, "states", len(states))
    run.expect(key, "tz_groups", len(groups))
    run.expect(key, "hours_per_group", 24)
    if not preflight(run, key, local_date, states, policy):
        return 0
    try:
        stations = load_stations(run)
    except Exception as e:
        run.error(key, "HRRR station_bg", f"station list unavailable: {e}")
        stations = None
    la = lo = None
    stored = 0

    for tzname, group_states in sorted(groups.items()):
        try:
            dmax, d40, d58, used = group_daily_max(tzname, local_date)
        except Exception as e:
            run.error(key, f"tz {tzname}", f"{type(e).__name__}: {e}",
                      details={"states": group_states})
            for st in group_states:
                run.day(key)["failed"].append(st)
            continue
        run.receive(key, "tz_groups")
        run.set_received(key, f"hours {tzname}", used)
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
        # (KEPT FOR PARITY: p_append=false deletes the whole date - plan T7)
        if stations is not None:
            try:
                bg_rows = []
                gset = {NAME2ABBR[s] for s in group_states}
                for stn in stations:
                    if stn.get("state") not in gset:
                        continue
                    j = int(np.argmin((la - stn["lat"])**2 + (lo - stn["lon"])**2))
                    yy, xx = np.unravel_index(j, la.shape)
                    bg_rows.append({"stid": stn["stid"],
                                    "bg": int(round(float(dmax[yy, xx])))})
                if any(not (0 <= r["bg"] <= 300) for r in bg_rows):
                    raise fg.ValidationError("station background outside 0-300 mph")
                run.write(key, f"HRRR station_bg {tzname}",
                          [("hz_station_bg_ingest",
                            {"p_secret": run.secret, "p_date": date_iso, "p_src": "HRRR",
                             "p_rows": bg_rows[i:i+3000], "p_append": i > 0})
                           for i in range(0, len(bg_rows), 3000)])
                run.receive(key, "station_bg_rows", len(bg_rows))
            except Exception as e:
                run.error(key, f"HRRR station_bg {tzname}", f"{type(e).__name__}: {e}")

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
                validate_state(st, geom, pts, cpts, used)
                calls = [("hz_bg_coarse_ingest",
                          {"p_secret": run.secret, "p_date": date_iso,
                           "p_src": "HRRR", "p_points": cpts[i:i+4000]})
                         for i in range(0, len(cpts), 4000)]
                calls += [("hz_hrrr_ingest",
                           {"p_secret": run.secret, "p_state": st, "p_date": date_iso,
                            "p_hours": used, "p_points": pts[i:i+4000],
                            "p_append": i > 0})
                          for i in range(0, len(pts), 4000)]
                run.write(key, st, calls)
                if not pts:
                    run.empty(key, st)
                    continue
                run.written(key, st)
                stored += 1
                print(f"  {date_iso}  {st:16s} {len(pts)} HRRR cells >= "
                      f"{FLOOR:.0f} mph ({used}/24 hrs)")
            except Exception as ex:
                run.error(key, st, f"{type(ex).__name__}: {ex}")
    for k, note in sorted(HOUR_NOTES.items()):
        run.note(key, note)
    HOUR_NOTES.clear()
    if stored == 0 and not run.day(key)["failed"]:
        print(f"{date_iso}: no HRRR wind >= {FLOOR:.0f} mph on land.")
    return stored


# ===================================================================== v4
# DAY_CONVENTION=v4 (Archive Phase 5 Stage 3). Used only when DAY_CONVENTION=v4.

LOWER48_ZONE_IDS = range(1, 22)     # tz MANIFEST ids 1..21 = the lower-48 zones


def v4_hours(window, zone, local_date, dst_fix=True, hour_ending=False):
    """UTC hour-ending times of one zone window.
    dst_fix (T3): the real consecutive UTC hours of the local day, start ..
    end-1h (23/24/25). dst_fix=False reproduces v3 local_hours() exactly (24
    wall-clock steps: on spring-forward days one UTC hour appears twice, on
    fall-back days the repeated hour is skipped). hour_ending (T12): every
    time +1 h (the hours ending 01:00 .. 24:00 local)."""
    start, end = window
    if dst_fix:
        n = int(round((end - start).total_seconds() / 3600))
        hours = [start + dt.timedelta(hours=h) for h in range(n)]
    else:
        hours = local_hours(zone, local_date)
    if hour_ending:
        hours = [t + dt.timedelta(hours=1) for t in hours]
    return hours


def max_over_hours(hours):
    """Daily max (mph) + d40/d58 over the given hour-ending times (same math as
    group_daily_max). Every hour must be present (raises otherwise)."""
    dmax = d40 = d58 = None
    for t in hours:
        v = hrrr_hour(t)
        mph = v * MS2MPH
        if dmax is None:
            dmax = mph.copy()
            d40 = (mph >= 40).astype("int16")
            d58 = (mph >= 58).astype("int16")
        else:
            np.fmax(dmax, mph, out=dmax)
            d40 += (mph >= 40).astype("int16")
            d58 += (mph >= 58).astype("int16")
    return dmax, d40, d58, len(hours)


def process_local_date_v4(run, local_date, states, policy="strict", flags=None, hour_ending=False):
    flags = flags or {"zones": "real", "dst": True}
    real = flags["zones"] == "real"
    date_iso = f"{local_date[:4]}-{local_date[4:6]}-{local_date[6:]}"
    key = date_iso
    run.expect(key, "states", len(states))
    zm = tzwin.zone_map("hrrr")
    # Preflight over every lower-48 zone window (same hours as the state zones on CONUS).
    dg48 = tzwin.DayGroups(local_date, LOWER48_ZONE_IDS)
    need = sorted({t for g in range(len(dg48.groups))
                   for t in v4_hours(dg48.groups[g], dg48.members[g][0], local_date, flags["dst"], hour_ending)}
                  | {t for s in states for t in v4_hours(tzwin.local_window(STATE_TZ[s], local_date),
                                                           STATE_TZ[s], local_date, flags["dst"], hour_ending)})
    if not preflight(run, key, local_date, states, policy, need=need):
        return 0
    try:
        stations = load_stations(run)
    except Exception as e:
        run.error(key, "HRRR station_bg", f"station list unavailable: {e}")
        stations = None

    st_zone = {st: tzwin.zone_id(STATE_TZ[st]) for st in states}
    if real and _LATLON is None:      # grid lat/lon arrive with the first decoded field
        try:
            hrrr_hour(need[0])
        except fg.FeedError:
            pass
    if real and _LATLON is None:
        run.error(key, "HRRR grid", "no HRRR hour could be decoded")
        return 0
    st_zones = {}
    for st in states:
        zs = set()
        if real:
            la, lo = _LATLON
            minx, miny, maxx, maxy = load_state_geom(st)[0].bounds
            m = (lo >= minx) & (lo <= maxx) & (la >= miny) & (la <= maxy)
            zs = set(np.unique(zm.zone[m]).tolist()) - {0}
        st_zones[st] = zs | {st_zone[st]}
    dg = tzwin.DayGroups(local_date, set().union(*st_zones.values()))
    st_group = {st: tzwin.group_of_window(dg, tzwin.local_window(STATE_TZ[st], local_date)) for st in states}
    st_groups = {st: sorted({int(dg.lut[z]) for z in st_zones[st]}) for st in states}
    used_g = sorted({g for st in states for g in st_groups[st]})
    run.day(key)["v4_groups"] = dg.describe()
    run.expect(key, "tz_groups", len(used_g))

    fields, failed_g = {}, {}
    for g in used_g:
        try:
            rep = dg.members[g][0]
            if not real:   # state mode: the group's state zone itself (v3 local_hours identity)
                rep = next(STATE_TZ[s] for s in states if st_group[s] == g)
            fields[g] = max_over_hours(v4_hours(dg.groups[g], rep, local_date, flags["dst"], hour_ending))
            run.receive(key, "tz_groups")
            run.set_received(key, f"hours {dg.members[g][0]}", fields[g][3])
        except Exception as e:
            failed_g[g] = f"{type(e).__name__}: {e}"
            run.error(key, f"tz {dg.members[g][0]}", failed_g[g],
                      details={"states": [s for s in states if g in st_groups[s]]})
    la, lo = _LATLON if _LATLON is not None else (None, None)

    cache = {}

    def field_for(st):
        """(dmax, d40, d58, candidate arrays, coarse arrays) for a state's cells.
        Real mode: every cell takes its own zone's window; cells with no US zone
        (never stored) keep the state's own window (= v3). One composite per
        state window group."""
        k = ("comp", st_group[st]) if real else st_group[st]
        if k not in cache:
            if real:
                gmap = dg.lut[zm.zone]
                own = fields[st_group[st]]
                dmax = tzwin.compose(gmap, {g: f[0] for g, f in fields.items()}, own[0])
                d40 = tzwin.compose(gmap, {g: f[1] for g, f in fields.items()}, own[1])
                d58 = tzwin.compose(gmap, {g: f[2] for g, f in fields.items()}, own[2])
                hours_cell = tzwin.compose(gmap, {g: np.full(dmax.shape, f[3], dtype="int16")
                                                  for g, f in fields.items()}, own[3])
            else:
                dmax, d40, d58, n = fields[k]
                hours_cell = None
            cg_v = dmax[::8, ::8]; cg_la = la[::8, ::8]; cg_lo = lo[::8, ::8]
            cyy, cxx = np.where(cg_v >= 5)
            ys, xs = np.where(dmax >= FLOOR)
            cache[k] = {"dmax": dmax,
                        "cand": (lo[ys, xs], la[ys, xs], dmax[ys, xs], d40[ys, xs], d58[ys, xs],
                                 zm.zone[ys, xs], None if hours_cell is None else hours_cell[ys, xs]),
                        "coarse": (cg_lo[cyy, cxx], cg_la[cyy, cxx], cg_v[cyy, cxx])}
        return cache[k]

    # T7: station backgrounds ONCE per date for every station, own-zone values.
    if stations is not None:
        try:
            gset = {NAME2ABBR[s] for s in states}
            rows = []
            for stn in stations:
                if stn.get("state") not in gset:
                    continue
                # the station's state decides which field (state mode) / composite (real)
                st_name = next(n for n, a in NAME2ABBR.items() if a == stn["state"])
                if st_name not in st_group:
                    continue
                if any(g in failed_g for g in (st_groups[st_name] if real else [st_group[st_name]])):
                    raise fg.ValidationError(f"zone window for {stn['stid']} ({stn['state']}) unavailable")
                dmax = field_for(st_name)["dmax"]
                j = int(np.argmin((la - stn["lat"])**2 + (lo - stn["lon"])**2))
                yy, xx = np.unravel_index(j, la.shape)
                rows.append({"stid": stn["stid"], "bg": int(round(float(dmax[yy, xx])))})
            if any(not (0 <= r["bg"] <= 300) for r in rows):
                raise fg.ValidationError("station background outside 0-300 mph")
            run.write(key, "HRRR station_bg",
                      [("hz_station_bg_ingest",
                        {"p_secret": run.secret, "p_date": date_iso, "p_src": "HRRR",
                         "p_rows": rows[i:i+3000], "p_append": i > 0})
                       for i in range(0, len(rows), 3000)])
            run.receive(key, "station_bg_rows", len(rows))
        except Exception as e:
            run.error(key, "HRRR station_bg", f"{type(e).__name__}: {e}")

    by_tz = {}
    for st in states:
        by_tz.setdefault(STATE_TZ[st], []).append(st)
    stored = 0
    for tzname, group_states in sorted(by_tz.items()):
        for st in group_states:
            bad = [g for g in (st_groups[st] if real else [st_group[st]]) if g in failed_g]
            if bad:
                run.day(key)["failed"].append(st)
                continue
            try:
                geom, pg = load_state_geom(st)
                minx, miny, maxx, maxy = geom.bounds
                F = field_for(st)
                c_lon, c_lat, c_v, c_d40, c_d58, c_zone, c_hours = F["cand"]
                cg_lonv, cg_latv, cg_vv = F["coarse"]
                own_hours = fields[st_group[st]][3]
                m = ((c_lon >= minx) & (c_lon <= maxx) & (c_lat >= miny) & (c_lat <= maxy))
                pts, max_h = [], own_hours
                for i in np.where(m)[0]:
                    x, y = float(c_lon[i]), float(c_lat[i])
                    if pg.contains(Point(x, y)):
                        if real and (int(c_zone[i]) == 0 or dg.lut[int(c_zone[i])] == tzwin.NO_GROUP):
                            raise fg.ValidationError(f"{st}: stored cell {x:.3f},{y:.3f} has no US zone")
                        if c_hours is not None:
                            max_h = max(max_h, int(c_hours[i]))
                        pts.append({"lon": round(x, 3), "lat": round(y, 3),
                                    "v": int(round(float(c_v[i]))),
                                    "d40": int(c_d40[i]),
                                    "d58": int(c_d58[i])})
                cm = ((cg_lonv >= minx) & (cg_lonv <= maxx) &
                      (cg_latv >= miny) & (cg_latv <= maxy))
                cpts = [{"lon": round(float(cg_lonv[i]), 2),
                         "lat": round(float(cg_latv[i]), 2),
                         "v": int(round(float(cg_vv[i])))}
                        for i in np.where(cm)[0]
                        if pg.contains(Point(float(cg_lonv[i]),
                                             float(cg_latv[i])))]
                validate_state(st, geom, pts, cpts, max_h)
                calls = [("hz_bg_coarse_ingest",
                          {"p_secret": run.secret, "p_date": date_iso,
                           "p_src": "HRRR", "p_points": cpts[i:i+4000]})
                         for i in range(0, len(cpts), 4000)]
                calls += [("hz_hrrr_ingest",
                           {"p_secret": run.secret, "p_state": st, "p_date": date_iso,
                            "p_hours": own_hours, "p_points": pts[i:i+4000],
                            "p_append": i > 0})
                          for i in range(0, len(pts), 4000)]
                run.write(key, st, calls)
                if not pts:
                    run.empty(key, st)
                    continue
                run.written(key, st)
                stored += 1
                print(f"  {date_iso}  {st:16s} {len(pts)} HRRR cells >= "
                      f"{FLOOR:.0f} mph ({own_hours} hrs{', ' + str(len(st_groups[st])) + ' zone windows' if len(st_groups[st]) > 1 else ''})")
            except Exception as ex:
                run.error(key, st, f"{type(ex).__name__}: {ex}")
    for k, note in sorted(HOUR_NOTES.items()):
        run.note(key, note)
    HOUR_NOTES.clear()
    if stored == 0 and not run.day(key)["failed"]:
        print(f"{date_iso}: no HRRR wind >= {FLOOR:.0f} mph on land.")
    return stored


def parse_states():
    states_env = (os.environ.get("STATES") or "").strip()
    if not states_env:
        return sorted(PERMITTED_STATES)
    states = [s.strip() for s in states_env.split(",") if s.strip()]
    bad = [s for s in states if s not in PERMITTED_STATES]
    if bad:
        raise fg.ValidationError(f"STATES has unknown/not permitted state(s): {bad}")
    return states


def main(run):
    explicit = fg.requested_dates()
    policy = "strict" if explicit is not None else "defer"
    if explicit is None:
        explicit = [dt.datetime.now(UTC).date() - dt.timedelta(days=1)]
    dates = [d.strftime("%Y%m%d") for d in explicit]
    states = parse_states()
    run.meta.update({"boundary_md5": BOUNDARY_MD5, "floor_mph": FLOOR,
                     "numpy": np.__version__, "pygrib": pygrib.__version__})
    conv = tzwin.convention()
    flags = tzwin.test_flags(run.dry_run) if conv == "v4" else None
    hour_ending = (os.environ.get("V4_HRRR_HOUR_ENDING") or "0").strip() == "1"
    if hour_ending and (conv != "v4" or not run.dry_run):
        raise fg.ValidationError("V4_HRRR_HOUR_ENDING=1 is a dry-run preview of plan item T12 "
                                 "(owner decision pending); it needs DAY_CONVENTION=v4 and DRY_RUN=1")
    run.meta["day_convention"] = conv
    if conv == "v4":
        run.meta.update({"tzwin_md5": tzwin.module_md5(), "zone_map_md5": tzwin.zone_map("hrrr").md5,
                         "v4_flags": flags, "hour_ending": hour_ending})
    print(f"HRRR ingest v2 ({conv} local-clock days): {len(dates)} date(s), "
          f"{len(states)} state(s){f' {flags}' if flags else ''}{' [T12 hour-ending]' if hour_ending else ''}")
    for d in dates:
        if conv == "v4":
            process_local_date_v4(run, d, states, policy, flags, hour_ending)
        else:
            process_local_date(run, d, states, policy)


if __name__ == "__main__":
    try:
        _run = fg.Run("hrrr")
    except fg.FeedError as e:
        print(f"::error::{e}")
        raise SystemExit(2)
    fg.main_guard(_run, main)
