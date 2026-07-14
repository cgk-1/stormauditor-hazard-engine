#!/usr/bin/env python3
"""
StormAuditor HAZARD ENGINE — observation & report layers ingester.

These are the layers that make the product evidence-backed instead of
model-only, mirroring what commercial suites bundle. All tiny compared to the
grids; each nightly run is a few MB.

  STATIONS  ASOS/AWOS station metadata for all CONUS state networks (weekly).
  DAILIES   Per-station daily peak wind gust (mph) for the target date(s) —
            the "nearest measured gust" layer (nightly; range mode for backfill:
            one request per network per year).
  LSR       NWS Local Storm Reports, national, wind + hail types, with the
            measured/estimated qualifier preserved (nightly or yearly).
  NCEI      Finalized NCEI Storm Events (Thunderstorm Wind, High Wind, Hail,
            Marine TW, Tornado) per year — the legally citable record
            (monthly refresh of current + prior year; once for history).
  HURDAT2   NHC Atlantic best track — tropical-day flagging (seasonal).

Env: SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET
Task selection: TASKS=stations,dailies,lsr,ncei,hurdat (default: dailies,lsr)
  DAILIES/LSR:  INGEST_DATE  YYYYMMDD | a:b range   (default yesterday UTC)
  NCEI:         NCEI_YEARS   e.g. "2022,2023,2024"  (default current year)
Deps: numpy requests
"""
import os, csv, gzip, io, json, re, time, datetime as dt
import urllib.request
import requests

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

LSR_WIND = {"TSTM WND GST","TSTM WND DMG","NON-TSTM WND GST","NON-TSTM WND DMG",
            "HIGH WIND","HURRICANE","TROPICAL STORM","DOWNBURST","MICROBURST",
            "TORNADO"}
LSR_HAIL = {"HAIL"}
NCEI_TYPES = {"Thunderstorm Wind":"wind","High Wind":"wind","Marine Thunderstorm Wind":"wind",
              "Hail":"hail","Marine Hail":"hail","Tornado":"wind",
              "Hurricane (Typhoon)":"wind","Tropical Storm":"wind"}


def _get(url, timeout=120, retries=3, binary=False):
    last = None
    for a in range(retries):
        try:
            raw = urllib.request.urlopen(
                urllib.request.Request(url, headers=UA), timeout=timeout).read()
            return raw if binary else raw.decode(errors="replace")
        except Exception as e:
            last = e; time.sleep(2 * (a + 1))
    raise RuntimeError(f"{url}: {last}")


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


def _chunks(rows, n=3000):
    for i in range(0, len(rows), n):
        yield i, rows[i:i + n]


# ------------------------------------------------------------------ stations
def task_stations(base, anon, secret):
    rows = []
    for st in CONUS:
        try:
            gj = json.loads(_get(f"{IEM}/geojson/network/{st}_ASOS.geojson"))
            for f in gj.get("features", []):
                lon, lat = f["geometry"]["coordinates"][:2]
                p = f["properties"]
                rows.append({"stid": f["id"], "name": p.get("sname", f["id"]),
                             "state": st, "lat": round(lat, 4),
                             "lon": round(lon, 4),
                             "network": f"{st}_ASOS"})
        except Exception as e:
            print(f"  [warn] stations {st}: {e}")
        time.sleep(0.1)
    for i, ch in _chunks(rows):
        rpc(base, anon, "hz_stations_ingest",
            {"p_secret": secret, "p_rows": ch, "p_append": i > 0})
    print(f"stations: {len(rows)} upserted")


# ------------------------------------------------------------------- dailies
def task_dailies(base, anon, secret, d0, d1):
    rows = []
    for st in CONUS:
        url = (f"{IEM}/cgi-bin/request/daily.py?network={st}_ASOS&stations=_ALL"
               f"&year1={d0.year}&month1={d0.month}&day1={d0.day}"
               f"&year2={d1.year}&month2={d1.month}&day2={d1.day}"
               f"&var=max_wind_gust_kts&format=csv&na=blank")
        try:
            for line in _get(url, timeout=300).splitlines()[1:]:
                p = line.split(",")
                if len(p) < 3 or not p[2].strip():
                    continue
                try:
                    mph = float(p[2]) * KT2MPH
                except ValueError:
                    continue
                if mph >= 20:
                    rows.append({"stid": p[0], "date": p[1],
                                 "gust_mph": round(mph, 1)})
        except Exception as e:
            print(f"  [warn] dailies {st}: {e}")
        time.sleep(0.15)
    for i, ch in _chunks(rows):
        rpc(base, anon, "hz_station_daily_ingest",
            {"p_secret": secret, "p_d0": d0.isoformat(), "p_d1": d1.isoformat(),
             "p_rows": ch, "p_append": i > 0})
    print(f"dailies {d0}..{d1}: {len(rows)} station-day gusts >= 20 mph")


# ----------------------------------------------------------------------- lsr
def task_lsr(base, anon, secret, d0, d1):
    url = (f"{IEM}/cgi-bin/request/gis/lsr.py?sts={d0:%Y-%m-%d}T00:00Z"
           f"&ets={d1:%Y-%m-%d}T23:59Z&fmt=csv")
    rows = []
    try:
        rdr = csv.reader(io.StringIO(_get(url, timeout=600)))
        hdr = next(rdr)
        ix = {k: hdr.index(k) for k in ("VALID","LAT","LON","MAG","TYPETEXT",
                                        "CITY","STATE","SOURCE","QUALIFIER")
              if k in hdr}
        for p in rdr:
            try:
                tt = p[ix["TYPETEXT"]].strip().upper()
                if tt not in LSR_WIND and tt not in LSR_HAIL:
                    continue
                rows.append({
                    "time_utc": p[ix["VALID"]],
                    "lat": round(float(p[ix["LAT"]]), 4),
                    "lon": round(float(p[ix["LON"]]), 4),
                    "kind": "hail" if tt in LSR_HAIL else "wind",
                    "type": tt, "mag": (p[ix["MAG"]] or None),
                    "city": p[ix["CITY"]][:80], "state": p[ix["STATE"]][:2],
                    "source": p[ix["SOURCE"]][:40],
                    "measured": p[ix["QUALIFIER"]].strip().upper() == "M"
                                if "QUALIFIER" in ix else False})
            except (ValueError, IndexError, KeyError):
                continue
    except Exception as e:
        print(f"  [warn] lsr: {e}")
        return
    for i, ch in _chunks(rows):
        rpc(base, anon, "hz_lsr_ingest",
            {"p_secret": secret, "p_d0": d0.isoformat(), "p_d1": d1.isoformat(),
             "p_rows": ch, "p_append": i > 0})
    print(f"lsr {d0}..{d1}: {len(rows)} wind/hail reports")


# ---------------------------------------------------------------------- ncei
def task_ncei(base, anon, secret, years):
    listing = _get(NCEI_DIR, timeout=180)
    for yr in years:
        m = re.findall(rf'(StormEvents_details-ftp_v1\.0_d{yr}_c\d+\.csv\.gz)',
                       listing)
        if not m:
            print(f"  [warn] ncei {yr}: file not found"); continue
        raw = gzip.decompress(_get(NCEI_DIR + sorted(m)[-1], binary=True,
                                   timeout=600))
        rdr = csv.DictReader(io.StringIO(raw.decode(errors="replace")))
        rows = []
        for r in rdr:
            et = r.get("EVENT_TYPE", "")
            if et not in NCEI_TYPES:
                continue
            try:
                lat = float(r["BEGIN_LAT"]); lon = float(r["BEGIN_LON"])
            except (ValueError, KeyError):
                continue
            rows.append({
                "event_id": r["EVENT_ID"], "kind": NCEI_TYPES[et],
                "event_type": et,
                "begin_utc": f'{r["BEGIN_YEARMONTH"]}{int(r["BEGIN_DAY"]):02d}'
                             f'{int(r["BEGIN_TIME"]):04d}',
                "lat": round(lat, 4), "lon": round(lon, 4),
                "magnitude": r.get("MAGNITUDE") or None,
                "mag_type": r.get("MAGNITUDE_TYPE") or None,   # MG=measured EG=estimated
                "state": r.get("STATE", "")[:24],
                "cz_name": r.get("CZ_NAME", "")[:60]})
        for i, ch in _chunks(rows):
            rpc(base, anon, "hz_storm_events_ingest",
                {"p_secret": secret, "p_year": int(yr), "p_rows": ch,
                 "p_append": i > 0})
        print(f"ncei {yr}: {len(rows)} finalized events")


# -------------------------------------------------------------------- hurdat
def task_hurdat(base, anon, secret):
    listing = _get(HURDAT_DIR, timeout=120)
    m = re.findall(r'(hurdat2-1851-\d{4}-\d+\.txt)', listing)
    if not m:
        print("  [warn] hurdat file not found"); return
    txt = _get(HURDAT_DIR + sorted(m)[-1], timeout=300)
    rows, sid, name = [], None, None
    for line in txt.splitlines():
        p = [x.strip() for x in line.split(",")]
        if len(p) >= 3 and p[0][:2] in ("AL", "EP", "CP") and p[0][2:].isdigit() is False:
            pass
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
    for i, ch in _chunks(rows):
        rpc(base, anon, "hz_hurdat_ingest",
            {"p_secret": secret, "p_rows": ch, "p_append": i > 0})
    print(f"hurdat: {len(rows)} track points (2015+)")


def main():
    base = os.environ["SUPABASE_URL"].rstrip("/")
    anon = os.environ["SUPABASE_ANON_KEY"]
    secret = os.environ["INGEST_SECRET"]
    tasks = (os.environ.get("TASKS") or "dailies,lsr").split(",")
    raw = os.environ.get("INGEST_DATE") or \
        (dt.datetime.now(UTC).date() - dt.timedelta(days=1)).strftime("%Y%m%d")
    if ":" in raw:
        a, b = raw.split(":")
        d0 = dt.datetime.strptime(a, "%Y%m%d").date()
        d1 = dt.datetime.strptime(b, "%Y%m%d").date()
    else:
        d0 = d1 = dt.datetime.strptime(raw.split(",")[0], "%Y%m%d").date()

    if "stations" in tasks:
        task_stations(base, anon, secret)
    if "dailies" in tasks:
        task_dailies(base, anon, secret, d0, d1)
    if "lsr" in tasks:
        task_lsr(base, anon, secret, d0, d1)
    if "ncei" in tasks:
        years = (os.environ.get("NCEI_YEARS") or
                 str(dt.date.today().year)).split(",")
        task_ncei(base, anon, secret, years)
    if "hurdat" in tasks:
        task_hurdat(base, anon, secret)


if __name__ == "__main__":
    main()
