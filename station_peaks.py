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
"""
import csv, datetime as dt, io, json, time, urllib.parse, urllib.request
from zoneinfo import ZoneInfo

import feedguard as fg

IEM = "https://mesonet.agron.iastate.edu"
UA = {"User-Agent": "StormAuditor-HazardEngine/1.0 (station peak times)"}
KT2MPH = 1.15078
MIN_MPH = 25.0
_tz_cache: dict[str, dict[str, str]] = {}


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
