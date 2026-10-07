#!/usr/bin/env python3
"""
Budget-safe backfill walker for the HRRR layer (v2, local-clock days).
Walks BACKWARD from yesterday to 2 years, one local date at a time, cursor
saved after EVERY COMPLETED date (key='hzhrrr'), same proven pattern as the
Explorer walkers. ~28 cached hourly downloads per date for all 48 states.

2026-10-07 (Archive Phase 5, Stage 1): stops on the first date that did not
ingest completely and never moves the cursor past it (it used to log and
advance, leaving permanent silent holes); the run exits non-zero.
This workflow is disabled; the change only matters if it is re-enabled.
Env: SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET
Optional: TIME_BUDGET_MIN (100), START_DATE/END_DATE (YYYYMMDD), DRY_RUN
"""
import os, time, datetime as dt
import feedguard as fg
import hz_hrrr_ingest as hz


def walk(run):
    t0 = time.time()
    budget = 60 * int(os.environ.get("TIME_BUDGET_MIN", "100"))
    today = dt.date.today()
    end = dt.datetime.strptime(os.environ["END_DATE"], "%Y%m%d").date() \
        if os.environ.get("END_DATE") else today - dt.timedelta(days=1)
    start = dt.datetime.strptime(os.environ["START_DATE"], "%Y%m%d").date() \
        if os.environ.get("START_DATE") else today - dt.timedelta(days=730)
    r = run.rpc_read("hz_backfill_get", {"p_key": "hzhrrr", "p_secret": run.secret})
    cur = r.json() if r.text and r.text != "null" else None
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
        n = hz.process_local_date(run, day.strftime("%Y%m%d"), states)
        if run.day(day.isoformat())["status"] == "error":
            print(f"  [STOP] {day} did not ingest completely; cursor stays at {cursor}."); break
        print(f"  {day}: {n} state-day(s) [{int(time.time()-t0)}s]")
        if not run.dry_run:
            run._post("hz_backfill_set", {"p_secret": run.secret, "p_key": "hzhrrr",
                                          "p_value": day.strftime("%Y-%m-%d")}, 60)
        cursor = day
        done += 1


if __name__ == "__main__":
    try:
        _run = fg.Run("hrrr-backfill")
    except fg.FeedError as e:
        print(f"::error::{e}")
        raise SystemExit(2)
    fg.main_guard(_run, walk)
