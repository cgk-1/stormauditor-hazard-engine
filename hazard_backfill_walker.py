#!/usr/bin/env python3
"""
Timeout-proof backfill walker for the HAZARD ENGINE wind layer (ANL + HRRR).

Same design as the proven feed walkers: walks BACKWARD from yesterday to
4 years, one 06Z convective day at a time, against a wall-clock budget
(default 100 min), saving the cursor to Supabase after EVERY day so scheduled
runs resume exactly where the last stopped. Newest history fills first, so
reports get deeper every day the walker runs.

Env: SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET
Optional: TIME_BUDGET_MIN (100), START_DATE/END_DATE (YYYYMMDD),
          SOURCES (default ANL,HRRR)
"""
import os, json, time, datetime as dt
import requests
import hazard_wind_ingest as hz


def rpc(base, anon, name, payload):
    r = requests.post(f"{base}/rest/v1/rpc/{name}",
        headers={"apikey": anon, "Authorization": f"Bearer {anon}",
                 "Content-Type": "application/json"},
        data=json.dumps(payload), timeout=60)
    if r.status_code >= 300:
        raise RuntimeError(f"{name} {r.status_code}: {r.text[:200]}")
    return r.json() if r.text and r.text != "null" else None


def main():
    t0 = time.time()
    budget = 60 * int(os.environ.get("TIME_BUDGET_MIN", "100"))
    base = os.environ["SUPABASE_URL"].rstrip("/")
    anon = os.environ["SUPABASE_ANON_KEY"]
    secret = os.environ["INGEST_SECRET"]
    sources = tuple((os.environ.get("SOURCES") or "ANL,HRRR").split(","))

    today = dt.date.today()
    end = dt.datetime.strptime(os.environ["END_DATE"], "%Y%m%d").date() \
        if os.environ.get("END_DATE") else today - dt.timedelta(days=1)
    start = dt.datetime.strptime(os.environ["START_DATE"], "%Y%m%d").date() \
        if os.environ.get("START_DATE") else today - dt.timedelta(days=730)

    cur = rpc(base, anon, "hz_backfill_get", {"p_key": "hzwind"})
    cursor = dt.datetime.strptime(cur, "%Y-%m-%d").date() if cur \
        else end + dt.timedelta(days=1)

    done = 0
    print(f"Hazard wind walker. Budget {budget//60} min. "
          f"Resuming before {cursor}. Floor {start}. Sources {sources}.")
    while True:
        day = cursor - dt.timedelta(days=1)
        if day < start:
            print(f"Hazard wind backfill COMPLETE: reached {start}.")
            break
        if time.time() - t0 > budget:
            print(f"Budget reached after {done} day(s). "
                  f"Next run resumes before {cursor}.")
            break
        try:
            n = hz.process_day(day.strftime("%Y%m%d"), base, anon, secret,
                               sources)
            print(f"  {day}: {n} cells stored [{int(time.time()-t0)}s]")
        except Exception as e:
            print(f"  [error] {day}: {e} -- advancing past it")
        rpc(base, anon, "hz_backfill_set",
            {"p_secret": secret, "p_key": "hzwind",
             "p_value": day.strftime("%Y-%m-%d")})
        cursor = day
        done += 1


if __name__ == "__main__":
    main()
