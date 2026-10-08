#!/usr/bin/env python3
"""
StormAuditor HAZARD ENGINE — observation & report layers ingester.

These are the layers that make the product evidence-backed instead of
model-only, mirroring what commercial suites bundle. All tiny compared to the
grids; each nightly run is a few MB.

  STATIONS  ASOS/AWOS station metadata for all CONUS state networks (weekly).
  PEAKS     WHEN each station's peak gust happened (local day, >= 25 mph) ->
            hz_station_peak (station_peaks.py; 2026-10-06, report time data).
  DAILIES   Per-station daily peak wind gust (mph) for the target date(s) —
            the "nearest measured gust" layer (nightly; range mode for backfill:
            one request per network per range).
  LSR       NWS Local Storm Reports, national, wind + hail types, with the
            measured/estimated qualifier preserved (nightly or yearly).
  NCEI      Finalized NCEI Storm Events (Thunderstorm Wind, High Wind, Hail,
            Marine TW, Tornado) per year — the legally citable record
            (monthly refresh of current + prior year; once for history).
  HURDAT2   NHC Atlantic best track — tropical-day flagging (seasonal).

STAGE 1 HARDENING (Archive Phase 5, 2026-10-07) - payloads are byte-identical
to the previous version for well-formed upstream replies:
  * Every IEM/NCEI/NHC fetch has a timeout, retries with backoff + jitter and
    polite pacing (serial, >= 0.25 s apart per IEM host).
  * Per-network failures (stations, dailies, peaks) are no longer swallowed:
    complete networks are written, the failed ones are listed, the job exits
    non-zero. NOTE dailies: hz_station_daily_ingest deletes the whole date
    range first, so a failed network has no rows for that range until a re-run.
  * Replies are validated: the IEM daily.py CSV header (IEM answers HTTP 200
    "ERROR: Invalid network specified" for DC, which has no ASOS network - that
    one is expected and noted; any other error text fails the network), the
    LSR CSV header, LSR rows (12-digit VALID, numeric in-range LAT/LON, 2-char
    STATE), NCEI rows. LSR rows that IEM emits with an unquoted comma in a
    free-text field (seen 3x in May 2024: "7220 NW 101 Terrace, Ka") used to
    be read with every later column shifted (wrong state); they are now
    repaired only when the repair is unambiguous and validated, otherwise
    quarantined. Unparseable rows are quarantined, never dropped silently.
  * Explicit dates: DATE=YYYY-MM-DD | START..END (comma lists are now all
    processed; the old code silently used only the first date).
  * DRY_RUN=1, completeness summary and the FEED_RESULT json line: feedguard.py.

STAGE 4 (owner-approved 2026-10-07): the workflows pass DAY_CONVENTION=v4 on every
schedule/dispatch unless a dispatch sets day_convention=v3 (the code's own default
stays v3). Readers and the engine keep reading the v3 tables until a later
approved switch; T4 (hz_station_daily_v4) stays an extra table only.

DAY_CONVENTION=v4 (Archive Phase 5 Stage 3, 2026-10-07; default v3 = unchanged).
Every v3 table keeps receiving exactly the v3 payload; v4 adds rows to NEW
additive tables (migration 20261007150000_phase5_v4_obs.sql in the site repo):
  * LSR (T1/T8) -> hz_lsr_v4 via hz_lsr_ingest_v4: each report's local date in
    the zone of ITS OWN POINT (tzpoint.py = offline mirror of hz_tz_at; fallback
    hz_state_tz(state) only for points with no US zone, tz_src='state'), with
    the zone stored per row so displays can print the time in it. The fetch
    window is extended by one small extra request (D+1 09Z-12Z) so Hawaii and
    the Aleutians get their whole local evening.
  * Dailies (T4) -> hz_station_daily_v4 via hz_station_daily_ingest_v4: the
    station's daily max gust from the SAME METAR pull as the peaks task, on the
    station's own DST-aware local day (station_peaks.daily_v4_from_csv). v4
    therefore needs the peaks task (TASKS=dailies alone writes only the v3
    dailies and warns).
  * NCEI (T5) -> hz_storm_events_v4 via hz_storm_events_ingest_v4: NCEI BEGIN
    date/time are local STANDARD time of CZ_TIMEZONE; v4 stores the true UTC
    instant (begin - CZ_TIMEZONE offset) and the local date in the zone of the
    event point. The v3 row set is kept (rows without coordinates are skipped,
    as in v3).

Env: SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET
Task selection: TASKS=stations,dailies,peaks,lsr,ncei,hurdat (default: dailies,peaks,lsr)
DAY_CONVENTION=v3|v4 (default v3)
  DAILIES/LSR/PEAKS:  DATE / INGEST_DATE  (default yesterday UTC)
  NCEI:         NCEI_YEARS   e.g. "2022,2023,2024"  (default current year)
Deps: requirements.txt (exact pins)
"""
import os, csv, gzip, io, json, re, time, datetime as dt
from decimal import Decimal

from zoneinfo import ZoneInfo

import clearstep
import feedguard as fg
import tzwin

UA = {"User-Agent": "StormAuditor-HazardEngine/1.0"}
UTC = dt.timezone.utc
KT2MPH = 1.15078
IEM = "https://mesonet.agron.iastate.edu"
NCEI_DIR = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/"
HURDAT_DIR = "https://www.nhc.noaa.gov/data/hurdat/"

CONUS = ["AL","AZ","AR","CA","CO","CT","DE","FL","GA","ID","IL","IN","IA","KS",
         "KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
         "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT",
         "VT","VA","WA","WV","WI","WY","DC"]
# IEM has no DC_ASOS network (DCA is in VA_ASOS): its replies are empty/"invalid
# network" by design. Any OTHER network answering like that is an error.
EMPTY_NETWORKS = {"DC"}

LSR_WIND = {"TSTM WND GST","TSTM WND DMG","NON-TSTM WND GST","NON-TSTM WND DMG",
            "HIGH WIND","HIGH SUST WINDS","MARINE TSTM WIND","HURRICANE",
            "TROPICAL STORM","DOWNBURST","MICROBURST","TORNADO"}
# 2026-09-04: MARINE TSTM WIND = measured buoy/C-MAN coastal gusts (real
# 2-letter states, lat/lon, QUALIFIER=M) — previously dropped. HIGH SUST
# WINDS is IEM's actual typetext ("HIGH WIND" never matched upstream).
LSR_HAIL = {"HAIL"}
LSR_COLUMNS = ("VALID","LAT","LON","MAG","TYPETEXT","CITY","STATE","SOURCE","QUALIFIER")
LSR_FREE_TEXT = ("CITY", "COUNTY", "REMARK", "UGCNAME")   # where unquoted commas appear
NCEI_TYPES = {"Thunderstorm Wind":"wind","High Wind":"wind","Marine Thunderstorm Wind":"wind",
              "Hail":"hail","Marine Hail":"hail","Tornado":"wind",
              "Hurricane (Typhoon)":"wind","Tropical Storm":"wind"}
DAILY_HEADER = "station,day,max_wind_gust_kts,network"
DAILIES_MIN_ROWS_PER_DAY = fg.env_float("DAILIES_MIN_ROWS_PER_DAY", 800)   # observed 1,452-1,715
# v4 dailies come from METAR gust/PK WND remarks only, which stations report only when it is gusty:
# on calm days the count is legitimately far below the IEM daily count (2024-01-23: 725 station-days
# with all 49 networks answering, reproduced; 01-24 839, 01-25 891). With EVERY network answered, a
# count between V4_DAILY_HARD_MIN and 800 is a warning (written); below the hard floor nothing is written.
V4_DAILY_HARD_MIN = fg.env_float("V4_DAILY_HARD_MIN", 300)


def _get(url, timeout=120, retries=5, binary=False):
    raw = fg.http_get(url, headers=UA, timeout=timeout, retries=retries)
    return raw if binary else raw.decode(errors="replace")


def _chunks(rows, n=3000):
    for i in range(0, len(rows), n):
        yield i, rows[i:i + n]


# ------------------------------------------------------------------ stations
def task_stations(run):
    key = "stations"
    rows = []
    run.expect(key, "networks", len(CONUS))
    for st in CONUS:
        try:
            gj = json.loads(_get(f"{IEM}/geojson/network/{st}_ASOS.geojson"))
            if not isinstance(gj, dict) or not isinstance(gj.get("features"), list):
                raise fg.ValidationError("reply is not a GeoJSON FeatureCollection")
            got = []
            for f in gj.get("features", []):
                lon, lat = f["geometry"]["coordinates"][:2]
                if not (-180 <= lon <= 180 and -90 <= lat <= 90):
                    raise fg.ValidationError(f"station {f['id']} has coordinates {lon},{lat}")
                p = f["properties"]
                got.append({"stid": f["id"], "name": p.get("sname", f["id"]),
                            "state": st, "lat": round(lat, 4),
                            "lon": round(lon, 4),
                            "network": f"{st}_ASOS"})
            if not got and st not in EMPTY_NETWORKS:
                raise fg.ValidationError("network has no stations")
            rows += got
            run.receive(key, "networks")
        except Exception as e:
            run.error(key, f"stations {st}", f"{type(e).__name__}: {e}")
        time.sleep(0.1)
    run.set_received(key, "stations", len(rows))
    # hz_stations_ingest is an upsert: writing the complete networks is safe.
    run.write(key, "hz_stations", [("hz_stations_ingest",
                                    {"p_secret": run.secret, "p_rows": ch, "p_append": i > 0})
                                   for i, ch in _chunks(rows)])
    print(f"stations: {len(rows)} upserted")


# ------------------------------------------------------------------- dailies
def parse_daily_csv(st, text, d0, d1):
    """-> (rows, notes). Raises ValidationError on an unexpected reply."""
    lines = text.splitlines()
    if st in EMPTY_NETWORKS and (not lines or lines[0].startswith("ERROR: Invalid network")):
        return [], f"{st}: no IEM ASOS network (expected empty)"
    if not lines or lines[0].strip() != DAILY_HEADER:
        raise fg.ValidationError(f"daily.py {st}: unexpected reply {text[:100]!r}")
    rows, high = [], []
    for line in lines[1:]:
        p = line.split(",")
        if len(p) != 4:
            raise fg.ValidationError(f"daily.py {st}: {len(p)} fields in {line[:80]!r}")
        if not p[0] or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", p[1]) or not (d0.isoformat() <= p[1] <= d1.isoformat()):
            raise fg.ValidationError(f"daily.py {st}: bad station/day in {line[:80]!r}")
        if len(p) < 3 or not p[2].strip():
            continue                     # no gust reported that day (expected)
        try:
            mph = float(p[2]) * KT2MPH
        except ValueError:
            raise fg.ValidationError(f"daily.py {st}: non-numeric gust in {line[:80]!r}") from None
        if mph >= 3:   # store the full field: low gusts are
                       # valid OA evidence (they temper hot backgrounds)
            rows.append({"stid": p[0], "date": p[1],
                         "gust_mph": round(mph, 1)})
            if mph > 200:
                high.append(f"{p[0]} {p[1]} {mph:.0f} mph")
    return rows, (f"{st}: suspicious gusts kept as reported: {high[:5]}" if high else None)


def task_dailies(run, d0, d1):
    key = d0.isoformat() if d0 == d1 else f"{d0}..{d1}"
    rows = []
    run.expect(key, "networks", len(CONUS))
    for st in CONUS:
        url = (f"{IEM}/cgi-bin/request/daily.py?network={st}_ASOS&stations=_ALL"
               f"&year1={d0.year}&month1={d0.month}&day1={d0.day}"
               f"&year2={d1.year}&month2={d1.month}&day2={d1.day}"
               f"&var=max_wind_gust_kts&format=csv&na=blank")
        try:
            got, note = parse_daily_csv(st, _get(url, timeout=300), d0, d1)
            if note and "suspicious" in note:
                run.warn(key, note)
            elif note:
                run.note(key, note)
            if not got and st not in EMPTY_NETWORKS:
                run.warn(key, f"dailies {st}: 0 station-days with a gust")
            rows += got
            run.receive(key, "networks")
        except Exception as e:
            run.error(key, f"dailies {st}", f"{type(e).__name__}: {e}")
        time.sleep(0.15)
    ndays = (d1 - d0).days + 1
    run.set_received(key, "station_days", len(rows))
    if len(rows) < DAILIES_MIN_ROWS_PER_DAY * ndays:
        run.error(key, "dailies", f"only {len(rows)} station-day gusts for {ndays} day(s) "
                                  f"(< {DAILIES_MIN_ROWS_PER_DAY:.0f}/day): nothing written")
        return
    run.write(key, "hz_station_daily",
              [("hz_station_daily_ingest",
                {"p_secret": run.secret, "p_d0": d0.isoformat(), "p_d1": d1.isoformat(),
                 "p_rows": ch, "p_append": i > 0}) for i, ch in _chunks(rows)])
    run.written(key, "hz_station_daily")
    print(f"dailies {d0}..{d1}: {len(rows)} station-day gusts stored (full field, >= 3 mph)")


# --------------------------------------------------------------------- peaks
def task_peaks(run, d0, d1, v4=False):
    """Station peak-gust TIME per local day (station_peaks.py) for the same
    dates as the dailies -> hz_station_peak. Separate table: the dailies'
    delete/re-insert never touches it. One METAR request per network per day.
    v4: the same METAR text also gives hz_station_daily_v4 (plan T4)."""
    import station_peaks as SP
    n = 0
    day = d0
    while day <= d1:
        key = day.isoformat()
        rows, v4_rows, v4_fail, disagree = [], [], [], []
        run.expect(key, "peak_networks", len(CONUS))
        for st in CONUS:
            try:
                if v4:
                    # One METAR pull feeds both the v3 peaks (IEM station zones,
                    # exactly as peaks_for_day) and the v4 dailies (point zones).
                    tzm = SP.station_tz(st)
                    if not SP.station_meta(st):
                        got = []      # empty IEM network (DC): peaks_for_day fetches nothing either
                    else:
                        text = SP.fetch_metars(st, day)
                        got = SP.peaks_from_csv(text, day, tzm) if tzm else []
                        try:
                            zones, dis, unplaced = SP.daily_v4_zones(st, day, _tz_lookup())
                            disagree += dis
                            if unplaced:
                                run.note(key, f"daily v4 {st}: {len(unplaced)} station(s) without coordinates "
                                              f"or zone: {unplaced[:5]}")
                            v4_rows += SP.daily_v4_from_csv(text, day, zones)
                        except Exception as e:
                            v4_fail.append(st)
                            run.error(key, f"daily v4 {st}", f"{type(e).__name__}: {e}")
                    if st not in v4_fail:
                        run.receive(key, "daily_v4_networks")
                else:
                    got = SP.peaks_for_day(st, day)
                big = [r for r in got if not (0 <= r["peak_mph"] <= 250)]
                if big:
                    run.warn(key, f"peaks {st}: {len(big)} peak(s) outside 0-250 mph are dropped by "
                                  f"hz_station_peak_ingest: {big[:3]}")
                rows += got
                run.receive(key, "peak_networks")
            except Exception as e:
                run.error(key, f"peaks {st}", f"{type(e).__name__}: {e}")
                if v4 and st not in v4_fail:
                    v4_fail.append(st)      # no METARs for this network -> no v4 daily for the day
            time.sleep(0.3)
        # hz_station_peak_ingest is an upsert: writing complete networks is safe.
        run.write(key, "hz_station_peak", [("hz_station_peak_ingest",
                                            {"p_secret": run.secret, "p_rows": ch})
                                           for _, ch in _chunks(rows, 2000)])
        run.set_received(key, "peak_rows", len(rows))
        if v4:
            run.expect(key, "daily_v4_networks", len(CONUS))
            run.set_received(key, "daily_v4_rows", len(v4_rows))
            if disagree:
                run.note(key, f"daily v4: {len(disagree)} station(s) whose IEM zone has another UTC offset "
                              f"than their point zone (point zone used): {disagree[:8]}")
            # A station-day whose METAR max is outside the table's physical range (0-250 mph; e.g. a
            # mis-keyed "G260KT" METAR) would make hz_station_daily_ingest_v4 reject the WHOLE day.
            # Same rule as hz_station_peak (which drops > 250 mph with a warning): quarantine the
            # row (record + warning) and write the rest. (Stage 5, 2026-10-08: IFP 2024-06-28 266 mph,
            # NQX 2024-06-17 260 mph.)
            bad_v4 = [r for r in v4_rows if not (0 <= float(r.get("gust_mph") or 0) <= 250)]
            if bad_v4:
                v4_rows = [r for r in v4_rows if 0 <= float(r.get("gust_mph") or 0) <= 250]
                path = run.quarantine(key, "daily v4 range", "gust outside 0-250 mph (not written)", bad_v4)
                run.warn(key, f"daily v4: {len(bad_v4)} station-day(s) outside 0-250 mph NOT written "
                              f"(quarantine {path}): {[(r.get('stid'), r.get('gust_mph')) for r in bad_v4[:5]]}")
            if v4_fail:
                pass          # error already recorded; the v4 daily of this day is NOT written
            elif len(v4_rows) < V4_DAILY_HARD_MIN:
                run.error(key, "daily v4", f"only {len(v4_rows)} station-day gusts (< "
                                           f"{V4_DAILY_HARD_MIN:.0f}): nothing written")
            else:
                if len(v4_rows) < DAILIES_MIN_ROWS_PER_DAY:
                    run.warn(key, f"daily v4: only {len(v4_rows)} station-day METAR gusts (< "
                                  f"{DAILIES_MIN_ROWS_PER_DAY:.0f}) with all {len(CONUS)} networks answering "
                                  f"(calm day); written")
                run.write(key, "hz_station_daily_v4",
                          [("hz_station_daily_ingest_v4",
                            {"p_secret": run.secret, "p_d0": key, "p_d1": key,
                             "p_rows": ch, "p_append": i > 0}) for i, ch in _chunks(v4_rows)])
                run.written(key, "hz_station_daily_v4")
        n += len(rows)
        print(f"peaks {day}: {len(rows)} station-days >= {SP.MIN_MPH:g} mph with peak time")
        day += dt.timedelta(days=1)
    print(f"peaks {d0}..{d1}: {n} rows")


# ----------------------------------------------------------------------- lsr
def _lsr_row_ok(rec):
    if not re.fullmatch(r"\d{12}", rec.get("VALID", "")):
        return False
    try:
        la, lo = float(rec["LAT"]), float(rec["LON"])
    except (ValueError, KeyError):
        return False
    if not (-90 <= la <= 90 and -180 <= lo <= 180):
        return False
    if not re.fullmatch(r"[A-Za-z]{2}", rec.get("STATE", "")):
        return False
    if rec.get("QUALIFIER", "").strip().upper() not in ("M", "E", "U", ""):
        return False
    if "UGC" in rec and rec["UGC"] not in ("", "None") and not re.fullmatch(r"[A-Z]{2}[CZ]\d{3}", rec["UGC"]):
        return False
    return True


def lsr_record(hdr, p):
    """Map one CSV row to {column: value}. Rows with MORE fields than the
    header come from an unquoted comma inside a free-text field: try merging
    the extra fields into each free-text column; accept only when the valid
    candidates agree on every column we store except the city text (then the
    CITY merge is preferred). Returns (rec, repaired?) or (None, reason)."""
    if len(p) == len(hdr):
        return dict(zip(hdr, p)), False
    if len(p) < len(hdr):
        return None, f"{len(p)} fields < {len(hdr)}"
    extra = len(p) - len(hdr)
    cands = []
    for col in LSR_FREE_TEXT:
        if col not in hdr:
            continue
        j = hdr.index(col)
        merged = p[:j] + [",".join(p[j:j + extra + 1])] + p[j + extra + 1:]
        rec = dict(zip(hdr, merged))
        if _lsr_row_ok(rec):
            cands.append((col, rec))
    if not cands:
        return None, f"{len(p)} fields > {len(hdr)} and no valid repair"
    fp = {tuple(r[k] for k in LSR_COLUMNS if k != "CITY") for _, r in cands}
    if len(fp) != 1:
        return None, f"{len(p)} fields > {len(hdr)} and the repair is ambiguous"
    for col, rec in cands:
        if col == "CITY":
            return rec, True
    return cands[0][1], True


def task_lsr(run, d0, d1, v4=False):
    # 2026-09-04: fetch through d1+1 09:00Z (covers Pacific + margin) so the
    # end day's local EVENING rows are in this run's payload; hz_lsr_ingest
    # now deletes exactly [d0, d1] and keeps only rows whose state-local date
    # is in that range. (The old pairing deleted [d0-1, d1] but fetched from
    # d0 00:00Z — every nightly run destroyed the previous local day's
    # daytime reports.)
    key = d0.isoformat() if d0 == d1 else f"{d0}..{d1}"
    url = (f"{IEM}/cgi-bin/request/gis/lsr.py?sts={d0:%Y-%m-%d}T00:00Z"
           f"&ets={d1 + dt.timedelta(days=1):%Y-%m-%d}T09:00Z&fmt=csv")
    try:
        text = _get(url, timeout=600)
        rdr = csv.reader(io.StringIO(text))
        hdr = next(rdr, None)
        if not hdr or not set(LSR_COLUMNS) <= set(hdr):
            raise fg.ValidationError(f"LSR reply lacks columns "
                                     f"{sorted(set(LSR_COLUMNS) - set(hdr or []))} (starts {text[:80]!r})")
    except Exception as e:
        run.error(key, "lsr", f"{type(e).__name__}: {e}")
        return
    rows, bad, repaired, kinds, other = lsr_rows(hdr, rdr)
    run.set_received(key, "lsr_wind_rows", kinds["wind"])
    run.set_received(key, "lsr_hail_rows", kinds["hail"])
    run.set_received(key, "lsr_other_types", other)
    if repaired:
        run.note(key, f"lsr: repaired {len(repaired)} row(s) with an unquoted comma in a free-text "
                      f"field: {[','.join(r[:2] + r[7:9]) for r in repaired[:3]]}")
    if bad:
        # Warning, not error (review 2026-10-07): IEM occasionally emits a few
        # rows with a mangled STATE; they are quarantined in the run record and
        # the rest are written, but a red run on every rerun would be noise.
        run.warn(key, f"{len(bad)} wind/hail LSR row(s) could not be read (quarantined: "
                      f"{[b if isinstance(b, str) else str(b)[:80] for b in bad[:3]]}); the other "
                      f"{len(rows)} rows are written")
    run.write(key, "hz_lsr", [("hz_lsr_ingest",
                               {"p_secret": run.secret, "p_d0": d0.isoformat(), "p_d1": d1.isoformat(),
                                "p_rows": ch, "p_append": i > 0}) for i, ch in _chunks(rows)])
    run.written(key, "hz_lsr")
    print(f"lsr {d0}..{d1}: {len(rows)} wind/hail reports")
    if v4:
        task_lsr_v4(run, key, d0, d1, rows)


def lsr_rows(hdr, rdr):
    """Parse the IEM LSR CSV body -> (rows, bad, repaired, kinds, other)."""
    rows, bad, repaired, kinds, other = [], [], [], {"wind": 0, "hail": 0}, 0
    for p in rdr:
        rec, why = lsr_record(hdr, p)
        if rec is None:
            # only quarantine rows that could be wind/hail (a broken row of
            # another type is irrelevant to this layer, but is still counted)
            if any(x.strip().upper() in LSR_WIND | LSR_HAIL for x in p):
                bad.append({"reason": why, "row": p})
            else:
                other += 1
            continue
        tt = rec["TYPETEXT"].strip().upper()
        if tt not in LSR_WIND and tt not in LSR_HAIL:
            other += 1
            continue
        if not _lsr_row_ok(rec):
            bad.append({"reason": "invalid VALID/LAT/LON/STATE/QUALIFIER", "row": p})
            continue
        if why:
            repaired.append(p)
        kind = "hail" if tt in LSR_HAIL else "wind"
        kinds[kind] += 1
        rows.append({
            "time_utc": rec["VALID"],
            "lat": round(float(rec["LAT"]), 4),
            "lon": round(float(rec["LON"]), 4),
            "kind": kind,
            "type": tt, "mag": (rec["MAG"] or None),
            "city": rec["CITY"][:80], "state": rec["STATE"][:2],
            "source": rec["SOURCE"][:40],
            "measured": rec["QUALIFIER"].strip().upper() == "M"})
    return rows, bad, repaired, kinds, other


_LOOKUP = None


def _tz_lookup():
    global _LOOKUP
    if _LOOKUP is None:
        import tzpoint
        _LOOKUP = tzpoint.lookup()
    return _LOOKUP


def lsr_local(row):
    """(iana, tz_src, local date) of one LSR row in v4: the zone of its own
    point; hz_state_tz(state) only when the point has no US zone."""
    import tzpoint
    z = _tz_lookup().at(row["lat"], row["lon"])
    src = "point"
    if not z:
        z, src = tzpoint.state_tz_db(row["state"]), "state"
    t = row["time_utc"]
    u = dt.datetime(int(t[:4]), int(t[4:6]), int(t[6:8]), int(t[8:10]), int(t[10:12] or 0), tzinfo=UTC)
    return z, src, u.astimezone(ZoneInfo(z)).date()


def task_lsr_v4(run, key, d0, d1, rows):
    """hz_lsr_v4: the v3 rows plus the D+1 09Z-12Z tail, each dated in its own
    point's zone, keeping exactly the local dates d0..d1."""
    url = (f"{IEM}/cgi-bin/request/gis/lsr.py?sts={d1 + dt.timedelta(days=1):%Y-%m-%d}T09:00Z"
           f"&ets={d1 + dt.timedelta(days=1):%Y-%m-%d}T12:00Z&fmt=csv")
    try:
        text = _get(url, timeout=300)
        rdr = csv.reader(io.StringIO(text))
        hdr = next(rdr, None)
        if not hdr or not set(LSR_COLUMNS) <= set(hdr):
            raise fg.ValidationError(f"LSR tail reply lacks columns "
                                     f"{sorted(set(LSR_COLUMNS) - set(hdr or []))} (starts {text[:80]!r})")
        tail, bad, _rep, _k, _o = lsr_rows(hdr, rdr)
        if bad:
            run.warn(key, f"{len(bad)} wind/hail LSR tail row(s) could not be read (quarantined)")
        import tzpoint
        seen = {json.dumps(r, sort_keys=True) for r in rows}
        out, by_src, misfit = [], {"point": 0, "state": 0}, []
        for r in rows + [r for r in tail if json.dumps(r, sort_keys=True) not in seen]:
            z, src, ld = lsr_local(r)
            if d0 <= ld <= d1:
                out.append(dict(r, date=ld.isoformat(), tz=z, tz_src=src))
                by_src[src] += 1
                t = r["time_utc"]
                u = dt.datetime(int(t[:4]), int(t[4:6]), int(t[6:8]), int(t[8:10]), int(t[10:12] or 0), tzinfo=UTC)
                if src == "point" and not tzpoint.zone_fits_state(z, r["state"], u):
                    misfit.append(f"{r['city']} {r['state']} {r['lat']},{r['lon']} -> {z}")
        if misfit:
            # The report's coordinates are in a zone its state does not have (upstream
            # coordinate error, e.g. 2026-06-07 '3 SE Dwtn Spearfish SD' at 36.61,-118.21).
            # Kept with the zone of the point where it is plotted; listed loudly.
            run.warn(key, f"lsr v4: {len(misfit)} report(s) whose coordinates lie in a zone their "
                          f"state does not have (dated by the plotted point): {misfit[:5]}")
            run.set_received(key, "lsr_v4_zone_not_in_state", len(misfit))
        run.set_received(key, "lsr_v4_rows", len(out))
        run.set_received(key, "lsr_v4_tail_rows", len(tail))
        run.set_received(key, "lsr_v4_state_fallback", by_src["state"])
        run.write(key, "hz_lsr_v4", [("hz_lsr_ingest_v4",
                                      {"p_secret": run.secret, "p_d0": d0.isoformat(), "p_d1": d1.isoformat(),
                                       "p_rows": ch, "p_append": i > 0}) for i, ch in _chunks(out)])
        run.written(key, "hz_lsr_v4")
        print(f"lsr v4 {d0}..{d1}: {len(out)} reports on their own local dates "
              f"({by_src['state']} on the state zone: no US zone at the point)")
    except Exception as e:
        run.error(key, "lsr v4", f"{type(e).__name__}: {e}")


# ---------------------------------------------------------------------- ncei
CZ_TZ_RE = re.compile(r"^([A-Z]{3,4})(-?)(\d{1,2})$")     # EST-5, CST-6, ..., GST10 (east of UTC)
# Standard-time abbreviation -> DST-aware IANA zone, used ONLY for NCEI rows whose
# point has no US zone (far offshore). Others (SST, GST, ...) keep the v3 date.
CZ_FAMILY = {"EST": "America/New_York", "CST": "America/Chicago", "MST": "America/Denver",
             "PST": "America/Los_Angeles", "AKST": "America/Anchorage", "HST": "Pacific/Honolulu",
             "AST": "America/Puerto_Rico"}


def ncei_v4_row(row, cz):
    """The v4 fields of one NCEI row: true UTC from the CZ_TIMEZONE standard
    offset, local date in the zone of the event point."""
    m = CZ_TZ_RE.match((cz or "").strip().upper())
    if not m:
        raise fg.ValidationError(f"event {row['event_id']}: CZ_TIMEZONE {cz!r} not understood")
    off = (-1 if m.group(2) else 1) * int(m.group(3))          # standard UTC offset, e.g. -6
    if not -11 <= off <= 12:
        raise fg.ValidationError(f"event {row['event_id']}: CZ_TIMEZONE {cz!r} offset outside UTC-11..+12")
    b = row["begin_utc"]          # v3 name; really LOCAL STANDARD time YYYYMMDDHHMM
    local = dt.datetime(int(b[:4]), int(b[4:6]), int(b[6:8]), int(b[8:10]), int(b[10:12]))
    utc = (local - dt.timedelta(hours=off)).replace(tzinfo=UTC)
    import tzpoint
    z, src = _tz_lookup().at(row["lat"], row["lon"]), "point"
    if not z:
        z, src = CZ_FAMILY.get(m.group(1)), "cz"
    if z:
        d = utc.astimezone(ZoneInfo(z)).date()
    else:
        z, src, d = None, "none", local.date()
    return {"cz_timezone": cz.strip(), "begin_utc_true": utc.strftime("%Y-%m-%dT%H:%M:00Z"),
            "tz": z, "tz_src": src, "date_v4": d.isoformat()}


def task_ncei(run, years, v4=False):
    listing = _get(NCEI_DIR, timeout=180)
    for yr in years:
        key = f"ncei {yr}"
        try:
            m = re.findall(rf'(StormEvents_details-ftp_v1\.0_d{yr}_c\d+\.csv\.gz)', listing)
            if not m:
                today = dt.date.today()
                if int(yr) == today.year and today.month <= 4:
                    run.warn(key, f"no NCEI details file for {yr} yet (normal early in the year)")
                    continue
                raise fg.UpstreamMissing(f"NCEI details file for {yr} not found")
            raw = gzip.decompress(_get(NCEI_DIR + sorted(m)[-1], binary=True, timeout=600))
            rdr = csv.DictReader(io.StringIO(raw.decode(errors="replace")))
            need = {"EVENT_ID", "EVENT_TYPE", "BEGIN_LAT", "BEGIN_LON", "BEGIN_YEARMONTH",
                    "BEGIN_DAY", "BEGIN_TIME", "MAGNITUDE", "MAGNITUDE_TYPE", "STATE", "CZ_NAME"}
            if v4:
                need = need | {"CZ_TIMEZONE"}
            if not need <= set(rdr.fieldnames or []):
                raise fg.ValidationError(f"NCEI {yr} lacks columns {sorted(need - set(rdr.fieldnames or []))}")
            rows, bad, no_coords, czs = [], [], 0, []
            for r in rdr:
                et = r.get("EVENT_TYPE", "")
                if et not in NCEI_TYPES:
                    continue
                try:
                    lat = float(r["BEGIN_LAT"]); lon = float(r["BEGIN_LON"])
                except (ValueError, KeyError):
                    no_coords += 1      # zone-based events carry no point (expected)
                    continue
                try:
                    begin = (f'{r["BEGIN_YEARMONTH"]}{int(r["BEGIN_DAY"]):02d}'
                             f'{int(r["BEGIN_TIME"]):04d}')
                    if not re.fullmatch(r"\d{12}", begin):
                        raise ValueError(f"begin {begin!r}")
                except (ValueError, KeyError) as e:
                    bad.append({"event_id": r.get("EVENT_ID"), "error": str(e)})
                    continue
                rows.append({
                    "event_id": r["EVENT_ID"], "kind": NCEI_TYPES[et],
                    "event_type": et,
                    "begin_utc": begin,
                    "lat": round(lat, 4), "lon": round(lon, 4),
                    "magnitude": r.get("MAGNITUDE") or None,
                    "mag_type": r.get("MAGNITUDE_TYPE") or None,   # MG=measured EG=estimated
                    "state": r.get("STATE", "")[:24],
                    "cz_name": r.get("CZ_NAME", "")[:60]})
                czs.append(r.get("CZ_TIMEZONE"))
            run.set_received(key, "events", len(rows))
            run.set_received(key, "events_without_point", no_coords)
            if bad:
                run.error(key, "ncei rows", f"{len(bad)} NCEI row(s) unreadable; the rest are written",
                          details=bad[:200])
            run.write(key, "hz_storm_events", [("hz_storm_events_ingest",
                                                {"p_secret": run.secret, "p_year": int(yr), "p_rows": ch,
                                                 "p_append": i > 0}) for i, ch in _chunks(rows)])
            run.written(key, "hz_storm_events")
            print(f"ncei {yr}: {len(rows)} finalized events")
            if v4:
                v4_rows, v4_bad = [], []
                for row, cz in zip(rows, czs):
                    try:
                        v4_rows.append(dict(row, **ncei_v4_row(row, cz)))
                    except fg.ValidationError as e:
                        v4_bad.append({"event_id": row["event_id"], "error": str(e)})
                if v4_bad:
                    run.error(key, "ncei v4 rows", f"{len(v4_bad)} NCEI row(s) without a usable CZ_TIMEZONE; "
                                                   f"the v4 year is NOT written", details=v4_bad[:200])
                else:
                    moved = sum(1 for r in v4_rows if r["date_v4"] != f"{r['begin_utc'][:4]}-{r['begin_utc'][4:6]}-{r['begin_utc'][6:8]}")
                    run.set_received(key, "events_v4", len(v4_rows))
                    run.set_received(key, "events_v4_date_changed", moved)
                    run.write(key, "hz_storm_events_v4", [("hz_storm_events_ingest_v4",
                                                           {"p_secret": run.secret, "p_year": int(yr), "p_rows": ch,
                                                            "p_append": i > 0}) for i, ch in _chunks(v4_rows)])
                    run.written(key, "hz_storm_events_v4")
                    print(f"ncei v4 {yr}: {len(v4_rows)} events, {moved} on another local date than v3")
        except Exception as e:
            run.error(key, "ncei", f"{type(e).__name__}: {e}")


# -------------------------------------------------------------------- hurdat
def task_hurdat(run):
    key = "hurdat"
    listing = _get(HURDAT_DIR, timeout=120)
    m = re.findall(r'(hurdat2-1851-\d{4}-\d+\.txt)', listing)
    if not m:
        raise fg.UpstreamMissing("hurdat file not found")
    txt = _get(HURDAT_DIR + sorted(m)[-1], timeout=300)
    rows, sid, name = [], None, None
    for line in txt.splitlines():
        p = [x.strip() for x in line.split(",")]
        if len(p) == 4 and len(p[0]) == 8 and p[0][:2] in ("AL","EP","CP"):
            sid, name = p[0], p[1]
            continue
        if sid and len(p) >= 8 and len(p[0]) == 8 and p[0].isdigit():
            if int(p[0][:4]) < 2015:
                continue
            lat = float(p[4][:-1]) * (1 if p[4][-1] == "N" else -1)
            lon = float(p[5][:-1]) * (-1 if p[5][-1] == "W" else 1)
            rows.append({"storm_id": sid, "name": name,
                         "time_utc": p[0] + p[1].replace(" ", ""),
                         "status": p[3], "lat": round(lat, 2),
                         "lon": round(lon, 2),
                         "wind_kt": int(p[6]) if p[6].lstrip("-").isdigit() else None})
    if len(rows) < 1000:
        raise fg.ValidationError(f"only {len(rows)} HURDAT track points since 2015 (expected thousands)")
    run.set_received(key, "track_points", len(rows))
    run.write(key, "hz_hurdat", [("hz_hurdat_ingest",
                                  {"p_secret": run.secret, "p_rows": ch, "p_append": i > 0})
                                 for i, ch in _chunks(rows)])
    run.written(key, "hz_hurdat")
    print(f"hurdat: {len(rows)} track points (2015+)")


def date_runs(dates):
    """Contiguous [d0, d1] runs of the requested dates (one range = one call)."""
    runs = []
    for d in sorted(dates):
        if runs and d == runs[-1][1] + dt.timedelta(days=1):
            runs[-1][1] = d
        else:
            runs.append([d, d])
    return [tuple(r) for r in runs]


def main(run):
    tasks = [t.strip() for t in (os.environ.get("TASKS") or "dailies,peaks,lsr").split(",") if t.strip()]
    unknown = set(tasks) - {"stations", "dailies", "peaks", "lsr", "ncei", "hurdat"}
    if unknown:
        raise fg.ValidationError(f"unknown TASKS {sorted(unknown)}")
    explicit = fg.requested_dates()
    dates = explicit if explicit is not None else [dt.datetime.now(UTC).date() - dt.timedelta(days=1)]
    run.meta["tasks"] = tasks
    conv = tzwin.convention()
    v4 = conv == "v4"
    run.meta["day_convention"] = conv
    if v4:
        if "dailies" in tasks and "peaks" not in tasks:
            # Stage 4: v4 is the nightly default, so a manual dailies-only re-pull must
            # keep working. The v3 dailies (hz_station_daily) are written as always; the
            # v4 daily comes from the peaks task's METAR pull, so it is NOT written here.
            run.warn(f"{dates[0]}", "TASKS has dailies without peaks: hz_station_daily (v3) is written; "
                                    "hz_station_daily_v4 is not (it comes from the peaks task's METAR pull - "
                                    "add peaks to refresh it)")
        if any(t in tasks for t in ("peaks", "lsr", "ncei")):
            run.meta.update({"tzwin_md5": tzwin.module_md5(), "tz_poly_fingerprint": _tz_lookup().fingerprint})

    if "stations" in tasks:
        try:
            task_stations(run)
        except Exception as e:
            run.error("stations", "stations", f"{type(e).__name__}: {e}")
    if any(t in tasks for t in ("dailies", "peaks", "lsr")):
        # Redo shadow (explicit-date runs only): snapshot every requested date's obs rows
        # (v3 + v4 tables) before anything is written; a date range with a failed
        # snapshot is not written at all (clearstep.py).
        snap_ok = {d: clearstep.before_day(run, d.isoformat(), "OBS", explicit=explicit is not None)
                   for d in dates}
        for d0, d1 in date_runs(dates):
            if not all(snap_ok[d0 + dt.timedelta(days=i)] for i in range((d1 - d0).days + 1)):
                run.error(f"{d0}", "redo-snapshot", f"obs {d0}..{d1}: a date of this range has no snapshot; "
                                                    f"the range is NOT written")
                continue
            if "dailies" in tasks:
                try:
                    task_dailies(run, d0, d1)
                except Exception as e:
                    run.error(f"{d0}", "dailies", f"{type(e).__name__}: {e}")
            if "peaks" in tasks:
                try:   # peak times are an add-on: never let them block the lsr task
                    task_peaks(run, d0, d1, v4)
                except Exception as e:
                    run.error(f"{d0}", "peaks", f"{type(e).__name__}: {e}")
            if "lsr" in tasks:
                try:
                    task_lsr(run, d0, d1, v4)
                except Exception as e:
                    run.error(f"{d0}", "lsr", f"{type(e).__name__}: {e}")
    if "ncei" in tasks:
        years = [y.strip() for y in (os.environ.get("NCEI_YEARS") or
                                     str(dt.date.today().year)).split(",") if y.strip()]
        task_ncei(run, years, v4)
    if "hurdat" in tasks:
        try:
            task_hurdat(run)
        except Exception as e:
            run.error("hurdat", "hurdat", f"{type(e).__name__}: {e}")


if __name__ == "__main__":
    try:
        _run = fg.Run("obs")
    except fg.FeedError as e:
        print(f"::error::{e}")
        raise SystemExit(2)
    fg.main_guard(_run, main)
