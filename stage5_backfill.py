#!/usr/bin/env python3
"""stage5_backfill.py - Archive Phase 5, Stage 5 (owner-approved 2026-10-07: "yes do that
workflow"; "fast track ... get moving on the archive plan now").

TEMPORARY, SELF-DISABLING backfill driver, run by .github/workflows/stage5-backfill.yml.
It backfills 2021-10-01 -> the day before each source's existing data in the v4 (own-zone
local-day) convention and rolls every backfilled grid day straight into the archive.

How (per lane step, newest missing days first, walking backward):
  1. gates: quiet UTC window, no feed workflow queued/running in the three feed repos, DB
     size <= MAX_DB_GB (20), < 7 active DB backends, lane contiguous with the archive.
  2. ingest a small batch (<= 3 days; OBS <= 7) with the FEED'S OWN ENTRYPOINT, exactly as
     the nightly workflows run it: DATE=<a..b> (explicit dates -> strict hours, redo shadow,
     clear-step), DAY_CONVENTION=v4, HRRR FLOOR_MPH=35, same pinned requirements, the feed
     repos' main branches (= the nightly code).
  3. read the feed's FEED_RESULT (result.json): a day counts only when its status is ok or
     warning, nothing failed, and (live) the redo post-write row counts matched the payload.
  4. live: hz_p5_roll_v1(lane, day) per day, newest first (service_role; sanity -> the same
     hz_arch_roll(src, day, true) the nightly roll uses -> cursor, one transaction). The
     nightly archive-roll never sees days older than the archive start, so this step is what
     keeps the raw tables from re-bloating. Contiguity is enforced in SQL.
  5. any failure stops the run (red). The same lane-day failing 3 runs in a row, a DB above
     the size guard, a missing migration/key or an inconsistent cursor DISABLES the workflow
     (feed-out/stage5/DISABLE -> the workflow's last step calls the GitHub API).
  6. every lane past 2021-10-01 and NCEI 2021-2023 done -> "STAGE 5 COMPLETE" -> disables
     itself the same way.

Lanes (cursor hz_backfill 'p5_<lane>' = oldest finished day):
  HAIL  hail-feed mesh_ingest.py       2023-09-27 -> 2021-10-01   archive src HAIL
  ANL   wind-feed wind_ingest.py       2024-07-22 -> 2021-10-01   archive src ANL + BGA
  HRRR  hazard    hz_hrrr_ingest.py    2024-07-24 -> 2021-10-01   archive src HRRR + BGH
  OBS   hazard    hazard_obs_ingest.py 2024-07-21 -> 2021-10-01   dailies, peaks, lsr (+ v4 tables)
                                                                  (2024-07-21 without lsr: hz_lsr has it)
  NCEI  hazard    hazard_obs_ingest.py years 2023, 2022, 2021     hz_storm_events + _v4

RUN_MODE != live (or DRY_RUN=1): computes everything on the runner and writes NOTHING (no feed write, no roll, no
cursor, no fail counter, never disables). With DATES=... the listed days run for every lane in
LANES (window gate off when IGNORE_WINDOW=1); without DATES the next planned batch per lane runs.

Env: SUPABASE_URL, SUPABASE_ANON_KEY, INGEST_SECRET, SUPABASE_SERVICE_ROLE_KEY (live only),
GH_TOKEN, GITHUB_REPOSITORY, GITHUB_RUN_ID, HAIL_DIR, WIND_DIR, HAZARD_DIR, OUT_DIR,
RUN_MODE (live|dry, default dry), DRY_RUN, DATES, LANES, TIME_BUDGET_MIN (50), IGNORE_WINDOW, MAX_DB_GB (20), MAX_ACTIVE (6).
"""
import datetime as dt
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

UTC = dt.timezone.utc
FLOOR = dt.date(2021, 10, 1)
OWNER = "cgk-1"
WORKFLOW_FILE = "stage5-backfill.yml"
REDO_PREFIX = "stage5"

LANES = {
    "HAIL": {"repo": "HAIL_DIR", "script": "mesh_ingest.py", "ceil": dt.date(2023, 9, 27),
             "srcs": ["HAIL"], "batch": 3, "per_day": 3, "timeout": 40},
    "ANL": {"repo": "WIND_DIR", "script": "wind_ingest.py", "ceil": dt.date(2024, 7, 22),
            "srcs": ["ANL", "BGA"], "batch": 3, "per_day": 7, "timeout": 60},
    "HRRR": {"repo": "HAZARD_DIR", "script": "hz_hrrr_ingest.py", "ceil": dt.date(2024, 7, 24),
             "srcs": ["HRRR", "BGH"], "batch": 3, "per_day": 5, "timeout": 50, "env": {"FLOOR_MPH": "35"}},
    "OBS": {"repo": "HAZARD_DIR", "script": "hazard_obs_ingest.py", "ceil": dt.date(2024, 7, 21),
            "srcs": [], "batch": 7, "per_day": 3, "timeout": 50},
}
LANE_ORDER = ["HAIL", "ANL", "HRRR", "OBS"]
LSR_EXISTS_FROM = dt.date(2024, 7, 21)          # hz_lsr already holds 2024-07-21 -> never re-pulled
NCEI_YEARS = [2023, 2022, 2021]
NCEI_MIN = 2

# Quiet UTC windows (start, end) in minutes after midnight. Outside them nothing starts:
# feed dispatches 10:10/12:10/16:10, archive roll 05:23, nightly site build ~14:00-16:40.
WINDOWS = [(0, 5 * 60 + 15), (5 * 60 + 45, 8 * 60 + 30), (18 * 60, 23 * 60 + 30)]

# Workflows that must not overlap a backfill step (iron rule 5: one heavy worker at a time).
FEED_WORKFLOWS = {
    "stormauditor-hail-feed": {"hail-ingest-new.yml", "hail-gap-refill.yml", "hail-backfill-new.yml"},
    "stormauditor-wind-feed": {"wind-ingest-new.yml", "wind-backfill-new.yml", "bgwalk-new.yml"},
    "stormauditor-hazard-engine": {"daily-new.yml", "weekly-new.yml", "backfill-new.yml"},
}


def flag(name):
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def now():
    return dt.datetime.now(UTC)


class Driver:
    def __init__(self):
        # Live only when RUN_MODE=live (the workflow sets it for the schedule / dispatch dry_run=0)
        # and DRY_RUN is not set; everything else is a dry run.
        self.dry = (os.environ.get("RUN_MODE") or "").strip().lower() != "live" or flag("DRY_RUN")
        self.out = os.path.abspath(os.environ.get("OUT_DIR") or "feed-out")
        self.sdir = os.path.join(self.out, "stage5")
        os.makedirs(self.sdir, exist_ok=True)
        self.base = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
        self.anon = os.environ.get("SUPABASE_ANON_KEY") or ""
        self.service = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
        self.secret = os.environ.get("INGEST_SECRET") or ""
        self.gh = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""
        self.run_id = os.environ.get("GITHUB_RUN_ID") or "local"
        self.started = now()
        self.budget_s = 60 * float(os.environ.get("TIME_BUDGET_MIN") or 50)
        self.max_db = float(os.environ.get("MAX_DB_GB") or 20)
        self.max_active = int(os.environ.get("MAX_ACTIVE") or 6)
        self.lanes = [x.strip().upper() for x in (os.environ.get("LANES") or "HAIL,ANL,HRRR,OBS,NCEI").split(",")
                      if x.strip()]
        bad = [x for x in self.lanes if x not in LANES and x != "NCEI"]
        if bad:
            raise SystemExit(f"::error::unknown LANES {bad}")
        self.steps = []
        self.notes = []
        self.errors = []
        self.disable_reason = None
        self.status = None

    # ------------------------------------------------------------ output
    def note(self, msg):
        print(f"[stage5] {msg}", flush=True)
        self.notes.append(msg)

    def error(self, msg):
        print(f"::error::stage5: {msg}", flush=True)
        self.errors.append(msg)

    def disable(self, reason):
        """Ask the workflow's last step to disable the workflow (never in a dry run)."""
        if self.dry:
            self.note(f"DRY RUN - would DISABLE the workflow: {reason}")
            return
        self.disable_reason = reason
        with open(os.path.join(self.sdir, "DISABLE"), "w") as fh:
            fh.write(reason + "\n")
        print(f"::warning::stage5: the workflow will DISABLE itself: {reason}", flush=True)

    # ------------------------------------------------------------ HTTP
    def _rpc(self, name, payload, key, timeout=120):
        hdr = {"apikey": key, "Content-Type": "application/json"}
        if not key.startswith("sb_"):
            hdr["Authorization"] = f"Bearer {key}"
        req = urllib.request.Request(f"{self.base}/rest/v1/rpc/{name}", data=json.dumps(payload).encode(),
                                     headers=hdr, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                body = r.read().decode()
                return r.status, (json.loads(body) if body else None)
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()[:500]

    def rpc_anon(self, name, payload, timeout=60):
        last = None
        for attempt in range(4):
            try:
                code, body = self._rpc(name, payload, self.anon, timeout)
            except Exception as e:  # network
                code, body = None, f"{type(e).__name__}: {e}"
            if code is not None and code < 300:
                return body
            last = f"{name}: HTTP {code} {body}"
            if code in (400, 401, 403, 404):
                break
            time.sleep(3 * (attempt + 1))
        raise RuntimeError(last)

    def gh_get(self, path):
        req = urllib.request.Request(f"https://api.github.com/{path}",
                                     headers={"Authorization": f"Bearer {self.gh}",
                                              "Accept": "application/vnd.github+json",
                                              "X-GitHub-Api-Version": "2022-11-28"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())

    # ------------------------------------------------------------ gates
    def window_end(self, t=None):
        """Datetime at which the current quiet window closes, or None outside every window."""
        t = t or now()
        m = t.hour * 60 + t.minute
        for a, b in WINDOWS:
            if a <= m < b:
                return t.replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(minutes=b)
        return None

    def deadline(self):
        end = self.started + dt.timedelta(seconds=self.budget_s)
        if self.dry and flag("IGNORE_WINDOW"):
            return end
        w = self.window_end()
        return min(end, w) if w else None

    def busy(self):
        """Feed workflow runs queued or in progress in any feed repo -> list (empty = free)."""
        found = []
        for repo, files in FEED_WORKFLOWS.items():
            for st in ("in_progress", "queued", "waiting", "pending"):
                data = self.gh_get(f"repos/{OWNER}/{repo}/actions/runs?status={st}&per_page=50")
                for r in data.get("workflow_runs", []):
                    if os.path.basename(r.get("path") or "") in files:
                        found.append(f"{repo}/{os.path.basename(r['path'])} run {r['id']} {st}")
        return found

    def check_pins(self):
        """The three feeds run in one interpreter: every pin must agree with wind-feed's
        (the superset; same versions the nightlies use)."""
        def pins(d):
            out = {}
            with open(os.path.join(os.environ[d], "requirements.txt")) as fh:
                for line in fh:
                    m = re.match(r"^\s*([A-Za-z0-9_.\-]+)==([^\s#]+)", line)
                    if m:
                        out[m.group(1).lower()] = m.group(2)
            return out
        wind = pins("WIND_DIR")
        for d in ("HAIL_DIR", "HAZARD_DIR"):
            for k, v in pins(d).items():
                if wind.get(k) != v:
                    raise RuntimeError(f"requirements differ: {d} pins {k}=={v}, wind-feed {wind.get(k)}")

    def feed_shas(self):
        out = {}
        for d in ("HAIL_DIR", "WIND_DIR", "HAZARD_DIR"):
            try:
                out[d] = subprocess.run(["git", "-C", os.environ[d], "rev-parse", "HEAD"], capture_output=True,
                                        text=True, check=True).stdout.strip()
            except Exception as e:
                out[d] = f"unknown ({e})"
        return out

    # ------------------------------------------------------------ DB state
    def refresh_status(self):
        try:
            self.status = self.rpc_anon("hz_p5_status_v1", {"p_secret": self.secret})
        except Exception as e:
            self.status = None
            if self.dry:
                self.note(f"hz_p5_status_v1 unavailable ({str(e)[:160]}) - migration not applied yet; "
                          f"dry run continues without DB gates")
            else:
                raise
        return self.status

    def cursor(self, key):
        if self.status is not None:
            return (self.status.get("cursors") or {}).get(key)
        try:   # dry run before the migration: plain cursor read (anon, secret-gated, existing RPC)
            return self.rpc_anon("hz_backfill_get", {"p_key": key, "p_secret": self.secret})
        except Exception:
            return None

    def next_day(self, lane):
        c = self.cursor(f"p5_{lane.lower()}")
        return LANES[lane]["ceil"] if not c else dt.date.fromisoformat(c) - dt.timedelta(days=1)

    def next_ncei(self):
        c = self.cursor("p5_ncei")
        y = NCEI_YEARS[0] if not c else int(c[:4]) - 1
        return y if y >= NCEI_YEARS[-1] else None

    def lane_block(self, lane, d):
        """None when lane day d is contiguous with the archive of every source of the lane."""
        if self.status is None or not LANES[lane]["srcs"]:
            return None
        amin = self.status.get("arch_min") or {}
        for s in LANES[lane]["srcs"]:
            m = amin.get(s)
            if m is None or dt.date.fromisoformat(m) != d + dt.timedelta(days=1):
                return f"{s} archive starts {m}, next {lane} day {d}"
        return None

    def fail_count(self, tag):
        raw = self.cursor("p5_fail")
        try:
            f = json.loads(raw) if raw else {}
        except ValueError:
            f = {}
        return f.get("n", 0) if f.get("tag") == tag else 0

    def set_fail(self, tag, n):
        if self.dry:
            return
        try:
            self.rpc_anon("hz_backfill_set", {"p_secret": self.secret, "p_key": "p5_fail",
                                              "p_value": json.dumps({"tag": tag, "n": n, "run": self.run_id}) if n else ""})
        except Exception as e:
            self.error(f"could not store the fail counter: {e}")

    # ------------------------------------------------------------ one feed run
    def run_feed(self, lane, dates, extra_env=None):
        cfg = LANES.get(lane) or LANES["OBS"]
        cwd = os.environ[cfg["repo"]]
        tag = f"{lane.lower()}-{dates.replace('..', '_').replace(',', '_')}"
        fout = os.path.join(self.out, "feeds", tag)
        os.makedirs(fout, exist_ok=True)
        env = dict(os.environ)
        for k in ("DATE", "INGEST_DATE", "STATES", "TASKS", "NCEI_YEARS", "HOURS_POLICY", "BG_ONLY",
                  "V4_ZONES", "V4_DST", "V4_HRRR_HOUR_ENDING", "COLD_GUARD", "CLEAR_STEP", "REDO_SNAPSHOT",
                  "REDO_RELEASE", "HEAL_SHORT_WINDOWS_FROM"):
            env.pop(k, None)
        env.update({"DATE": dates, "DAY_CONVENTION": "v4", "DRY_RUN": "1" if self.dry else "",
                    "FEED_OUT_DIR": fout, "REDO_RUN_ID": f"{REDO_PREFIX}-{lane.lower()}",
                    "SUPABASE_URL": self.base, "SUPABASE_ANON_KEY": self.anon, "INGEST_SECRET": self.secret})
        env.pop("SUPABASE_SERVICE_ROLE_KEY", None)        # the feeds only ever use the anon key
        env.update(cfg.get("env") or {})
        env.update(extra_env or {})
        if not dates:
            env.pop("DATE")
        t0 = time.time()
        print(f"::group::{lane} {dates or ''} {extra_env or ''}", flush=True)
        try:
            p = subprocess.run([sys.executable, cfg["script"]], cwd=cwd, env=env, timeout=60 * cfg["timeout"])
            code = p.returncode
        except subprocess.TimeoutExpired:
            code = "timeout"
        print("::endgroup::", flush=True)
        res = None
        try:
            with open(os.path.join(fout, "result.json")) as fh:
                res = json.load(fh)
        except Exception as e:
            self.error(f"{lane} {dates}: no FEED_RESULT ({e}); exit {code}")
        return code, res, round(time.time() - t0)

    @staticmethod
    def day_ok(res, key, dry):
        d = next((x for x in res.get("days", []) if x.get("day") == key), None)
        if d is None:
            return False, "day missing from FEED_RESULT"
        if d.get("status") not in ("ok", "warning") or d.get("failed"):
            return False, f"status {d.get('status')} failed {d.get('failed')}"
        redo = d.get("redo") or {}
        if not dry:
            pc = redo.get("postcheck")
            if not (isinstance(pc, dict) and pc.get("ok")):
                return False, f"redo post-write check not ok: {pc}"
        return True, "ok"

    def summarize_feed(self, res):
        if not res:
            return None
        days = [{k: d.get(k) for k in ("day", "status", "failed", "rows", "payload_md5", "expected", "received")}
                | {"warnings": [w["msg"][:200] for w in res.get("warnings", []) if w.get("day") == d.get("day")],
                   "clear_step": (d.get("clear_step") or {}).get("would_clear", (d.get("clear_step") or {}).get("result")),
                   "redo": {k: (d.get("redo") or {}).get(k) for k in ("status", "dry_run", "postcheck")}}
                for d in res.get("days", [])]
        meta = res.get("meta") or {}
        return {"feed": res.get("feed"), "status": res.get("status"), "dry_run": res.get("dry_run"),
                "errors": res.get("errors"), "write_retries": res.get("write_retries"),
                "meta": {k: meta.get(k) for k in ("day_convention", "zone_map_md5", "tzwin_md5", "hours_policy",
                                                  "hour_ending", "cold_guard", "floor_mph", "tasks", "feedguard_md5")},
                "days": days}

    def roll(self, lane, day):
        if self.dry:
            self.note(f"DRY RUN - would roll {lane} {day} (hz_p5_roll_v1)")
            return {"ok": True, "status": "dry_run"}
        code, body = self._rpc("hz_p5_roll_v1", {"p_secret": self.secret, "p_lane": lane, "p_day": str(day),
                                                 "p_run_id": f"{REDO_PREFIX}-{self.run_id}",
                                                 "p_max_db_gb": self.max_db}, self.service, timeout=180)
        if code is None or code >= 300 or not isinstance(body, dict):
            return {"ok": False, "status": "http", "error": f"HTTP {code}: {body}"}
        return body

    # ------------------------------------------------------------ lane steps
    def grid_or_obs_step(self, lane, days):
        """days newest first. Returns True when every day succeeded (and, live, was rolled)."""
        cfg = LANES[lane]
        rng = f"{days[-1]}..{days[0]}" if len(days) > 1 else str(days[0])
        extra = None
        if lane == "OBS":
            extra = {"TASKS": "dailies,peaks" if days[0] >= LSR_EXISTS_FROM else "dailies,peaks,lsr"}
        code, res, secs = self.run_feed(lane, rng, extra)
        step = {"lane": lane, "dates": rng, "exit": code, "seconds": secs, "feed": self.summarize_feed(res),
                "rolled": [], "dry_run": self.dry}
        self.steps.append(step)
        if res is None:
            return False, days[0], "no FEED_RESULT"
        if lane == "OBS":
            if res.get("status") == "error" or res.get("errors"):
                return False, days[0], f"obs errors: {[e.get('msg', '')[:160] for e in res.get('errors', [])][:5]}"
            if not self.dry:
                for d in days:          # snapshot / post-write checks per date
                    dd = next((x for x in res.get("days", []) if x.get("day") == str(d)), {})
                    if (dd.get("redo") or {}).get("status") not in ("taken", "exists"):
                        return False, d, f"redo snapshot missing for {d}: {dd.get('redo')}"
        for d in days:                  # newest first: contiguous rolls
            if lane != "OBS":
                ok, why = self.day_ok(res, str(d), self.dry)
                if not ok:
                    return False, d, why
            r = self.roll(lane, d)
            step["rolled"].append({"day": str(d), "result": r})
            if not r.get("ok"):
                return False, d, f"roll {r.get('status')}: {r.get('error')}"
            if r.get("status") == "rolled_warn":
                print(f"::warning::stage5 {lane} {d}: row count outside 0.2x-5x of the recent median "
                      f"{r.get('warnings')}", flush=True)
        return True, None, None

    def ncei_step(self, year):
        code, res, secs = self.run_feed("NCEI", "", {"TASKS": "ncei", "NCEI_YEARS": str(year)})
        step = {"lane": "NCEI", "dates": str(year), "exit": code, "seconds": secs,
                "feed": self.summarize_feed(res), "rolled": [], "dry_run": self.dry}
        self.steps.append(step)
        if res is None or res.get("status") == "error" or res.get("errors"):
            return False, year, f"ncei errors: {(res or {}).get('errors')}"
        d = next((x for x in res.get("days", []) if x.get("day") == f"ncei {year}"), {})
        if set(d.get("written") or []) != {"hz_storm_events", "hz_storm_events_v4"}:
            return False, year, f"ncei {year}: written {d.get('written')}"
        r = self.roll("NCEI", dt.date(year, 1, 1))
        step["rolled"].append({"day": f"{year}-01-01", "result": r})
        if not r.get("ok"):
            return False, year, f"roll {r.get('status')}: {r.get('error')}"
        return True, None, None

    def plan_next(self):
        """(kind, lane, value) for the next step, or ('done', None, None) / ('wait', reason, None)."""
        if "NCEI" in self.lanes:
            y = self.next_ncei()
            if y is not None:
                return "ncei", "NCEI", y
        best, waits = None, []
        for lane in LANE_ORDER:
            if lane not in self.lanes:
                continue
            d = self.next_day(lane)
            if d < FLOOR:
                continue
            blk = self.lane_block(lane, d)
            if blk:
                if lane == "ANL" and d == dt.date(2024, 7, 22) and \
                        (self.status.get("arch_min") or {}).get("ANL") == "2024-07-24":
                    waits.append("ANL waits for the owner decision on ANL 2024-07-23 (go-live step)")
                    continue
                raise RuntimeError(f"lane {lane} is not contiguous with the archive: {blk}")
            if best is None or d > best[1]:
                best = (lane, d)
        if best:
            return "lane", best[0], best[1]
        if waits:
            return "wait", "; ".join(waits), None
        return "done", None, None

    # ------------------------------------------------------------ main
    def main(self):
        self.note(f"{'DRY RUN (nothing is written)' if self.dry else 'LIVE'}; lanes {self.lanes}; "
                  f"feeds {self.feed_shas()}")
        self.check_pins()
        if not self.dry and (os.environ.get("DATES") or flag("IGNORE_WINDOW")):
            raise RuntimeError("DATES / IGNORE_WINDOW are dry-run-only inputs")
        dl = self.deadline()
        if dl is None:
            self.note(f"outside the quiet UTC windows {WINDOWS} (minutes) - nothing to do this run")
            return 0
        try:
            busy = self.busy()
        except Exception as e:
            busy = [f"busy check failed: {type(e).__name__}: {e}"]
        if busy:
            if self.dry:
                self.note(f"feed workflows active (dry run continues, nothing is written): {busy}")
            else:
                self.note(f"yielding to feed workflows: {busy}")
                return 0
        if not self.dry and not self.service:
            self.disable("SUPABASE_SERVICE_ROLE_KEY secret missing (go-live step)")
            raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY is not set")
        try:
            self.refresh_status()
        except Exception as e:
            if "HTTP 404" in str(e):
                self.disable(f"hz_p5_status_v1 missing - migration not applied ({str(e)[:120]})")
            raise
        if self.status:
            self.note(f"DB {self.status['db_bytes'] / 2**30:.2f} GB, active {self.status['active']}, "
                      f"archive starts {self.status.get('arch_min')}, cursors {self.status.get('cursors')}")

        if self.dry and os.environ.get("DATES"):
            for lane in self.lanes:
                if lane == "NCEI":
                    years = sorted({int(x.strip()[:4]) for x in os.environ["DATES"].split(",") if x.strip()},
                                   reverse=True)
                    for y in years:
                        self.ncei_step(y)
                    continue
                days = sorted({dt.date.fromisoformat(x.strip()) for x in os.environ["DATES"].split(",") if x.strip()},
                              reverse=True)
                extra = {"TASKS": "dailies,peaks,lsr"} if lane == "OBS" else None
                code, res, secs = self.run_feed(lane, ",".join(str(d) for d in sorted(days)), extra)
                self.steps.append({"lane": lane, "dates": [str(d) for d in days], "exit": code, "seconds": secs,
                                   "feed": self.summarize_feed(res), "dry_run": True,
                                   "day_ok": {str(d): self.day_ok(res, str(d), True)[1] if res else None
                                              for d in days} if lane != "OBS" else None})
            return 1 if any((s.get("feed") or {}).get("status") == "error" or s.get("feed") is None
                            for s in self.steps) else 0

        while True:
            kind, lane, val = self.plan_next()
            if kind == "done":
                if set(self.lanes) >= set(LANE_ORDER) | {"NCEI"}:
                    self.note("STAGE 5 COMPLETE: every lane reached 2021-10-01 and NCEI 2021-2023 is loaded")
                    self.disable("Stage 5 backfill complete")
                else:
                    self.note(f"lanes {self.lanes} are complete (other lanes not checked in this run)")
                return 0
            if kind == "wait":
                print(f"::warning::stage5: {lane}", flush=True)
                return 0
            remaining = (dl - now()).total_seconds() / 60
            if kind == "ncei":
                if remaining < 20:
                    break
                ok, at, why = self.ncei_step(val)
            else:
                cfg = LANES[lane]
                k = min(cfg["batch"], (val - FLOOR).days + 1, int((remaining - 5) // cfg["per_day"]))
                if lane == "OBS" and val >= LSR_EXISTS_FROM:
                    k = min(k, 1)               # 2024-07-21 alone (no lsr task)
                if k < 1:
                    break
                days = [val - dt.timedelta(days=i) for i in range(k)]
                if lane == "OBS":
                    days = [d for d in days if d < LSR_EXISTS_FROM] or days[:1]
                if not self.dry:
                    try:
                        busy = self.busy()
                    except Exception as e:
                        busy = [f"busy check failed: {e}"]
                    if busy:
                        self.note(f"yielding to feed workflows: {busy}")
                        break
                    st = self.refresh_status()
                    if st["db_bytes"] > self.max_db * 2**30:
                        self.disable(f"DB {st['db_bytes'] / 2**30:.2f} GB > {self.max_db} GB guard")
                        raise RuntimeError("DB size guard")
                    if st["active"] > self.max_active:
                        self.note(f"DB busy ({st['active']} active backends) - yielding")
                        break
                ok, at, why = self.grid_or_obs_step(lane, days)
            if ok:
                if self.cursor("p5_fail"):
                    self.set_fail("", 0)
                if self.dry:
                    self.note("DRY RUN: one planned step done; stopping (cursors do not move in a dry run)")
                    break
                self.refresh_status()
                continue
            n = self.fail_count(f"{lane}:{at}") + 1
            self.error(f"{lane} step failed at {at}: {why} (attempt {n} of 3 for this day)")
            self.set_fail(f"{lane}:{at}", n)
            if n >= 3:
                self.disable(f"{lane} {at} failed 3 runs in a row: {why}")
            return 1
        self.note("time budget / quiet window used up - the next scheduled run continues")
        return 0

    def finish(self, code):
        res = {"status": "error" if (code or self.errors) else "ok", "dry_run": self.dry,
               "run_id": self.run_id, "started_utc": self.started.isoformat(), "finished_utc": now().isoformat(),
               "lanes": self.lanes, "steps": self.steps, "notes": self.notes, "errors": self.errors,
               "disable": self.disable_reason, "status_db": self.status}
        with open(os.path.join(self.sdir, "result.json"), "w") as fh:
            json.dump(res, fh, indent=1, default=str)
        sp = os.environ.get("GITHUB_STEP_SUMMARY")
        if sp:
            with open(sp, "a") as fh:
                fh.write(f"### Stage 5 backfill: {res['status']}{' (DRY RUN, nothing written)' if self.dry else ''}\n\n")
                fh.write("| lane | dates | exit | s | feed status | rolled |\n|---|---|---|---|---|---|\n")
                for s in self.steps:
                    fh.write(f"| {s['lane']} | {s['dates']} | {s['exit']} | {s['seconds']} | "
                             f"{(s.get('feed') or {}).get('status')} | "
                             f"{', '.join(r['day'] + ':' + str(r['result'].get('status')) for r in s.get('rolled', []))} |\n")
                for n in self.notes:
                    fh.write(f"- {n}\n")
                for e in self.errors:
                    fh.write(f"- **error** {e}\n")
        print("STAGE5_RESULT " + json.dumps(res, sort_keys=True, separators=(",", ":"), default=str), flush=True)
        return 1 if res["status"] == "error" else 0


if __name__ == "__main__":
    drv = Driver()
    try:
        rc = drv.main()
    except Exception as e:
        import traceback
        traceback.print_exc()
        drv.error(f"{type(e).__name__}: {e}")
        rc = 1
    sys.exit(drv.finish(rc))
