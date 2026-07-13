#!/usr/bin/env python3
"""
Budget-safe backfill walker for the HRRR layer (v2, local-clock days).
Walks BACKWARD from yesterday to 2 years, one local date at a time, cursor
saved after EVERY date (key='hzhrrr'), same proven pattern as the Explorer
walkers. ~28 cached hourly downloads per date for all 48 states.
Env: SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET
Optional: TIME_BUDGET_MIN (100), START_DATE/END_DATE (YYYYMMDD)
"""
import os, json, time, datetime as dt
import requests
import hz_hrrr_ingest as hz


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
    today = dt.date.today()
    end = dt.datetime.strptime(os.environ["END_DATE"], "%Y%m%d").date() \
        if os.environ.get("END_DATE") else today - dt.timedelta(days=1)
    start = dt.datetime.strptime(os.environ["START_DATE"], "%Y%m%d").date() \
        if os.environ.get("START_DATE") else today - dt.timedelta(days=730)
    cur = rpc(base, anon, "hz_backfill_get", {"p_key": "hzhrrr"})
    cursor = dt.datetime.strptime(cur, "%Y-%m-%d").date() if cur \
        else end + dt.timedelta(days=1)
    states = sorted(hz.PERMITTED_STATES)
    done = 0
    print(f"HRRR walker. Budget {budget//60} min. Resuming before {cursor}. "
          f"Floor {start}.")
    while True:
        day = cursor - dt.timedelta(days=1)
        if day < start:
            print(f"HRRR backfill COMPLETE: reached {start}."); break
        if time.time() - t0 > budget:
            print(f"Budget reached after {done} date(s). "
                  f"Next run resumes before {cursor}."); break
        try:
            n = hz.process_local_date(day.strftime("%Y%m%d"), states,
                                      base, anon, secret)
            print(f"  {day}: {n} state-day(s) [{int(time.time()-t0)}s]")
        except Exception as e:
            print(f"  [error] {day}: {e} -- advancing past it")
        rpc(base, anon, "hz_backfill_set",
            {"p_secret": secret, "p_key": "hzhrrr",
             "p_value": day.strftime("%Y-%m-%d")})
        cursor = day
        done += 1


if __name__ == "__main__":
    main()
