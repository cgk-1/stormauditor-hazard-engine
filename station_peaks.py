"""Station peak-gust TIME per local day (StormAuditor time-data, 2026-10-06).

hz_station_daily holds IEM's daily max gust with no time. This module reads
the station's METARs from IEM asos.py and finds WHEN the peak happened:

  candidates for local day D (station's own time zone, DST-aware):
    * METAR gust   -> event time = the METAR valid time        (src 1)
    * PK WND remark -> event time = the remark's hh:mm         (src 2)
  peak = highest value; ties -> earliest; remark preferred at equal time.
  A PK WND issued after midnight for a gust before midnight belongs to the
  day the gust HAPPENED (event-time bucketing).
  wind_hours = local clock hours of D with at least one wind observation
  (coverage: < 24 means gaps in the record). tz = the station's IANA zone,
  so the site can print the time in the station's local time.

Validated as the M7 study (docs/m7-localday-peaktimes-2026-10-01.md in the
site repo, 11,910 station-days). Display rule (site side): a time is shown
only when this peak is within 1 mph of the stored daily gust, so a number is
never paired with another reading's time.

Only station-days whose peak is >= MIN_MPH are returned (storage: these are
the readings reports use as evidence).

v4 (Archive Phase 5 Stage 3, plan T4): daily_v4_from_csv() derives each
station's DAILY max gust from the SAME METAR pull, on the station's own local
calendar day (zone from the hz_tz_at mirror at the station's coordinates,
DST-aware), for every station-day with a gust >= 3 mph (the hz_station_daily
floor). It replaces IEM daily.py's max_wind_gust_kts, whose day boundary
behaves like local STANDARD time for part of the network. Stored in the
additive table hz_station_daily_v4; hz_station_daily (v3) is unchanged.
"""
import csv, datetime as dt, io, json, time, urllib.parse, urllib.request
from zoneinfo import ZoneInfo

import feedguard as fg

IEM = "https://mesonet.agron.iastate.edu"
UA = {"User-Agent": "StormAuditor-HazardEngine/1.0 (station peak times)"}
KT2MPH = 1.15078
MIN_MPH = 25.0
_tz_cache: dict[str, dict[str, str]] = {}
_meta_cache: dict[str, dict[str, tuple]] = {}     # v4: {stid: (lat, lon, IEM tzname or None)}


def _get(url, timeout=300, retries=5):
    """IEM GET with timeout, retries, backoff + jitter and polite pacing."""
    return fg.http_get(url, headers=UA, timeout=timeout, retries=retries,
                       base_sleep=5).decode(errors="replace")


def station_tz(state: str) -> dict[str, str]:
    """{stid: tzname} for one state's ASOS network (IEM metadata)."""
    if state not in _tz_cache:
        gj = json.loads(_get(f"{IEM}/geojson/network/{state}_ASOS.geojson", timeout=120))
        if not isinstance(gj, dict) or not isinstance(gj.get("features"), list):
            raise fg.ValidationError(f"{state}_ASOS metadata is not a GeoJSON FeatureCollection")
        _tz_cache[state] = {f["id"]: f["properties"].get("tzname") for f in gj.get("features", [])
                            if f["properties"].get("tzname")}
        meta = {}
        for f in gj.get("features", []):
            try:
                lon, lat = (float(x) for x in f["geometry"]["coordinates"][:2])
            except (TypeError, ValueError, KeyError, IndexError):
                continue          # v4 reports stations it cannot place (daily_v4_zones)
            meta[f["id"]] = (lat, lon, f["properties"].get("tzname") or None)
        _meta_cache[state] = meta
        no_tz = [f["id"] for f in gj["features"] if not f["properties"].get("tzname")]
        if no_tz:
            print(f"  [note] {state}_ASOS: {len(no_tz)} station(s) without a time zone are not "
                  f"used for peak times: {no_tz[:10]}")
    return _tz_cache[state]


def fetch_metars(state: str, day: dt.date) -> str:
    """UTC D 03Z .. D+1 11Z covers every US local day D plus late PK WND remarks."""
    d1 = day + dt.timedelta(days=1)
    q = [("network", f"{state}_ASOS")]
    q += [("data", v) for v in ("sknt", "gust", "peak_wind_gust", "peak_wind_time")]
    q += [("tz", "Etc/UTC"), ("format", "onlycomma"), ("latlon", "no"), ("missing", "empty"),
          ("trace", "empty"), ("report_type", "3"), ("report_type", "4"),
          ("year1", day.year), ("month1", day.month), ("day1", day.day), ("hour1", 3),
          ("year2", d1.year), ("month2", d1.month), ("day2", d1.day), ("hour2", 11)]
    return _get(f"{IEM}/cgi-bin/request/asos.py?" + urllib.parse.urlencode(q))


def _utc(s: str) -> dt.datetime:
    return dt.datetime.strptime(s[:16], "%Y-%m-%d %H:%M").replace(tzinfo=dt.timezone.utc)


def _num(s):
    """'' / None / 'M' = not reported. Anything else must be a number: an
    unparseable value is an unexpected format (2026-10-07: it used to be
    treated as 'not reported' silently)."""
    if s in ("", None, "M"):
        return None
    try:
        return float(s)
    except ValueError:
        raise fg.ValidationError(f"unexpected non-numeric METAR value {s!r}") from None


METAR_COLUMNS = {"station", "valid", "sknt", "gust", "peak_wind_gust", "peak_wind_time"}


def peaks_from_csv(text: str, day: dt.date, tzmap: dict[str, str], min_mph: float = MIN_MPH) -> list[dict]:
    by_st: dict[str, list[dict]] = {}
    rdr = csv.DictReader(io.StringIO(text))
    cols = set(rdr.fieldnames or [])
    if not METAR_COLUMNS <= cols:
        raise fg.ValidationError(f"asos.py reply lacks columns {sorted(METAR_COLUMNS - cols)} "
                                 f"(starts {text[:80]!r})")
    for r in rdr:
        st = r.get("station")
        if st and st in tzmap:
            by_st.setdefault(st, []).append(r)
    out = []
    for st, obs in by_st.items():
        tz = ZoneInfo(tzmap[st])
        cands = []          # (kt, event_time, src)
        seen_pk = set()
        hours = set()
        for o in obs:
            v = _utc(o["valid"])
            local = v.astimezone(tz)
            if local.date() == day and _num(o.get("sknt")) is not None:
                hours.add(local.hour)
            g = _num(o.get("gust"))
            if g is not None and local.date() == day:
                cands.append((g, v, 1))
            pk = _num(o.get("peak_wind_gust"))
            pt = o.get("peak_wind_time") or ""
            if pk is not None and pt:
                t = _utc(pt)
                if t.astimezone(tz).date() == day and (pk, t) not in seen_pk:
                    seen_pk.add((pk, t))
                    cands.append((pk, t, 2))
        if not cands:
            continue
        kt, t, src = sorted(cands, key=lambda c: (-c[0], c[1], c[2] != 2))[0]
        mph = round(kt * KT2MPH, 1)
        if mph < min_mph:
            continue
        out.append({"stid": st, "date": day.isoformat(), "peak_time_utc": t.strftime("%Y-%m-%dT%H:%M:00Z"),
                    "peak_mph": mph, "src": src, "wind_hours": len(hours), "tz": tzmap[st]})
    return out


def peaks_for_day(state: str, day: dt.date, min_mph: float = MIN_MPH) -> list[dict]:
    tz = station_tz(state)
    if not tz:   # empty IEM network (e.g. DC: DCA is in VA_ASOS) -> IEM reads it as an all-stations request (HTTP 400)
        return []
    return peaks_from_csv(fetch_metars(state, day), day, tz, min_mph)


# ------------------------------------------------------------------ v4 (T4)
DAILY_V4_MIN_MPH = 3.0     # same floor as hz_station_daily


def station_meta(state: str) -> dict[str, tuple]:
    """{stid: (lat, lon, IEM tzname or None)} from the same network fetch as station_tz."""
    station_tz(state)
    return _meta_cache[state]


def daily_v4_zones(state: str, day: dt.date, lookup) -> tuple[dict, list, list]:
    """{stid: (iana, tz_src)} for a network's stations. tz_src 'point' = the
    hz_tz_at mirror at the station's coordinates (source of truth, same as the
    grids); 'iem' = IEM's tzname, only when the point has no US zone.
    Also returns the stations whose IEM zone has a different UTC offset on
    `day` (reported, the point zone wins) and the stations that cannot be placed."""
    zones, disagree, unplaced = {}, [], []
    noon = dt.datetime(day.year, day.month, day.day, 12)
    for stid, (lat, lon, iem) in station_meta(state).items():
        z = lookup.at(lat, lon)
        if z:
            zones[stid] = (z, "point")
            if iem and iem != z:
                try:
                    if noon.replace(tzinfo=ZoneInfo(iem)).utcoffset() != noon.replace(tzinfo=ZoneInfo(z)).utcoffset():
                        disagree.append(f"{stid} point={z} iem={iem}")
                except Exception:
                    disagree.append(f"{stid} point={z} iem={iem} (unknown IEM zone)")
        elif iem:
            zones[stid] = (iem, "iem")
        else:
            unplaced.append(stid)
    return zones, disagree, unplaced


def daily_v4_from_csv(text: str, day: dt.date, zones: dict, min_mph: float = DAILY_V4_MIN_MPH) -> list[dict]:
    """Per-station daily max gust for local day `day` in the station's own zone.
    Candidates exactly as peaks_from_csv (METAR gust at its valid time, PK WND
    at its event time); gust_mph = the max of all of them (ties: earliest,
    remark first), gust_metar_mph = the max of the METAR gust field alone."""
    by_st: dict[str, list[dict]] = {}
    rdr = csv.DictReader(io.StringIO(text))
    cols = set(rdr.fieldnames or [])
    if not METAR_COLUMNS <= cols:
        raise fg.ValidationError(f"asos.py reply lacks columns {sorted(METAR_COLUMNS - cols)} "
                                 f"(starts {text[:80]!r})")
    for r in rdr:
        st = r.get("station")
        if st and st in zones:
            by_st.setdefault(st, []).append(r)
    out = []
    for st, obs in by_st.items():
        tzname, tz_src = zones[st]
        tz = ZoneInfo(tzname)
        cands, metar, seen_pk, hours, n = [], [], set(), set(), 0
        for o in obs:
            v = _utc(o["valid"])
            local = v.astimezone(tz)
            if local.date() == day:
                n += 1
                if _num(o.get("sknt")) is not None:
                    hours.add(local.hour)
            g = _num(o.get("gust"))
            if g is not None and local.date() == day:
                cands.append((g, v, 1))
                metar.append(g)
            pk = _num(o.get("peak_wind_gust"))
            pt = o.get("peak_wind_time") or ""
            if pk is not None and pt:
                t = _utc(pt)
                if t.astimezone(tz).date() == day and (pk, t) not in seen_pk:
                    seen_pk.add((pk, t))
                    cands.append((pk, t, 2))
        if not cands:
            continue
        kt, t, src = sorted(cands, key=lambda c: (-c[0], c[1], c[2] != 2))[0]
        mph = round(kt * KT2MPH, 1)
        if mph < min_mph:
            continue
        out.append({"stid": st, "date": day.isoformat(), "gust_mph": mph,
                    "gust_metar_mph": round(max(metar) * KT2MPH, 1) if metar else None,
                    "peak_time_utc": t.strftime("%Y-%m-%dT%H:%M:00Z"), "src": src,
                    "wind_hours": len(hours), "n_obs": n, "tz": tzname, "tz_src": tz_src})
    return out
