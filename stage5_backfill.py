#!/usr/bin/env python3
"""stage5_backfill.py - Archive Phase 5, Stage 5 (owner-approved 2026-10-07: "yes do that
workflow", "go, B", "make it MUCH faster").

TEMPORARY, SELF-DISABLING, PARALLEL backfill driver for .github/workflows/stage5-backfill.yml.
Backfills 2021-10-01 -> the day before each source's existing data in the v4 (own-zone
local-day) convention and rolls every backfilled grid day straight into the archive.

Three modes (MODE=plan | shard | roll), one workflow run = plan -> N parallel shards -> roll:

  plan   reads hz_p5_status_v1 (cursors, ready markers, archive starts, DB size, active
         backends), checks the gates and writes the shard matrix: for each lane the next
         pending days (not rolled, not marked ready), newest first, dealt round-robin to the
         lane's shards (shard i gets days i, i+S, i+2S, ...) so all shards advance together
         and the contiguous roll keeps up. A lane never runs more than LOOKAHEAD days ahead
         of its roll cursor (bounds the raw rows in flight).
  shard  for each assigned day, newest first: gates (forbidden windows, feed runs, DB load,
         DB size) -> the FEED'S OWN ENTRYPOINT for that day (DATE=day, DAY_CONVENTION=v4 ->
         strict hours, redo shadow, clear-step; HRRR FLOOR_MPH=35; same pins, feed main
         branches) -> FEED_RESULT check (ok/warning, nothing failed, redo post-write counts
         match) -> ready marker hz_backfill 'p5_ready_<lane>_<day>'. Shard 0 of each lane is the
         lane's roller: after each of its days it rolls every contiguous ready day.
         A failed day is skipped (others continue), counted in 'p5_fail_<lane>_<day>'; the
         3rd failed run of the same day disables the workflow.
  roll   after all shards: rolls every contiguous ready day of every lane.

Roll = hz_p5_roll_v1(lane, day) (service_role; sanity -> the nightly's hz_arch_roll(src, day,
true) -> cursor, one transaction) ONLY for cursor-1 and only when its ready marker exists:
ingest may run ahead, the archive stays strictly contiguous (the nightly roll never meets a hole).

Gates (never relaxed): no work 05:15-05:45, 09:00-11:00, 11:50-12:30, 15:50-17:00 UTC (a shard
stops before a day that would overlap them, and frees its runner); no work while any feed
workflow is queued/running in the three feed repos; DB > MAX_DB_GB (20) -> stop + disable;
active backends > MAX_ACTIVE -> back off (wait, then stop); same day failing 3 runs -> disable;
all lanes past 2021-10-01 + NCEI 2021-2023 -> "STAGE 5 COMPLETE" -> disable.

RUN_MODE != live (or DRY_RUN=1): everything is computed, NOTHING is written (no feed write,
marker, roll, cursor, counter; never disables). DATES=... (dry runs only) gives every shard of
every lane in LANES explicit days instead of the plan.
"""
import datetime as dt
import json
import os
import random
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request

UTC = dt.timezone.utc
FLOOR = dt.date(2021, 10, 1)
OWNER = "cgk-1"
REDO_PREFIX = "stage5"

# per_day = runner minutes per day measured in the 2026-10-07 dry run (ANL 4.2, OBS 2.6 with IEM
# 429 backoff, HRRR 1.4, HAIL 0.25) plus write time and margin.
LANES = {
    "HAIL": {"repo": "HAIL_DIR", "script": "mesh_ingest.py", "ceil": dt.date(2023, 9, 27),
             "srcs": ["HAIL"], "per_day": 1.0, "timeout": 30},
    "ANL": {"repo": "WIND_DIR", "script": "wind_ingest.py", "ceil": dt.date(2024, 7, 22),
            "srcs": ["ANL", "BGA"], "per_day": 5.5, "timeout": 40},
    "HRRR": {"repo": "HAZARD_DIR", "script": "hz_hrrr_ingest.py", "ceil": dt.date(2024, 7, 24),
             "srcs": ["HRRR", "BGH"], "per_day": 2.5, "timeout": 30, "env": {"FLOOR_MPH": "35"}},
    "OBS": {"repo": "HAZARD_DIR", "script": "hazard_obs_ingest.py", "ceil": dt.date(2024, 7, 21),
            "srcs": [], "per_day": 3.5, "timeout": 30},
}
LANE_ORDER = ["HAIL", "ANL", "HRRR", "OBS"]
LSR_EXISTS_FROM = dt.date(2024, 7, 21)          # hz_lsr already holds 2024-07-21: never re-pulled
NCEI_YEARS = [2023, 2022, 2021]
# Forbidden UTC windows (minutes after midnight): archive roll, feed dispatch windows.
FORBID = [(5 * 60 + 15, 5 * 60 + 45), (9 * 60, 11 * 60), (11 * 60 + 50, 12 * 60 + 30), (15 * 60 + 50, 17 * 60)]
FEED_WORKFLOWS = {
    "stormauditor-hail-feed": {"hail-ingest-new.yml", "hail-gap-refill.yml", "hail-backfill-new.yml"},
    "stormauditor-wind-feed": {"wind-ingest-new.yml", "wind-backfill-new.yml", "bgwalk-new.yml"},
    "stormauditor-hazard-engine": {"daily-new.yml", "weekly-new.yml", "backfill-new.yml"},
}


def flag(name):
    return (os.environ.get(name) or "").strip().lower() in ("1", "true", "yes", "on")


def now():
    return dt.datetime.now(UTC)


def shard_counts():
    """SHARDS env HAIL=1,ANL=9,HRRR=3,OBS=7 (defaults = 20 parallel jobs)."""
    out = {"HAIL": 1, "ANL": 9, "HRRR": 3, "OBS": 7}   # balanced by the run-1 rates (days/h per shard)
    for tok in (os.environ.get("SHARDS") or "").split(","):
        if "=" in tok:
            k, v = tok.split("=", 1)
            if k.strip().upper() in out:
                out[k.strip().upper()] = max(1, min(20, int(v)))
    return out


def minutes_to_forbidden(t=None):
    """0 inside a forbidden window, else minutes until the next one starts."""
    t = t or now()
    m = t.hour * 60 + t.minute + t.second / 60
    best = 24 * 60
    for a, b in FORBID:
        if a <= m < b:
            return 0
        d = (a - m) % (24 * 60)
        best = min(best, d)
    return best


class Driver:
    def __init__(self):
        self.mode = (os.environ.get("MODE") or "shard").strip().lower()
        self.dry = (os.environ.get("RUN_MODE") or "").strip().lower() != "live" or flag("DRY_RUN")
        self.out = os.path.abspath(os.environ.get("OUT_DIR") or "feed-out")
        self.sdir = os.path.join(self.out, "stage5")
        os.makedirs(self.sdir, exist_ok=True)
        self.base = (os.environ.get("SUPABASE_URL") or "").rstrip("/")
        self.anon = os.environ.get("SUPABASE_ANON_KEY") or ""
        self.service = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
        self.mgmt = (os.environ.get("SUPABASE_ACCESS_TOKEN") or "").strip()   # sandbox roller fallback
        self.secret = os.environ.get("INGEST_SECRET") or ""
        self.gh = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN") or ""
        self.run_id = os.environ.get("GITHUB_RUN_ID") or "local"
        self.started = now()
        self.budget_s = 60 * float(os.environ.get("TIME_BUDGET_MIN") or 300)
        self.max_db = float(os.environ.get("MAX_DB_GB") or 20)
        self.max_active = int(os.environ.get("MAX_ACTIVE") or 14)
        self.lookahead = int(os.environ.get("LOOKAHEAD") or 150)     # candidate list length per lane (plan)
        self.max_ahead = int(os.environ.get("MAX_AHEAD") or 12)      # HARD cap: un-rolled raw days per lane
        self.pause_db = float(os.environ.get("PAUSE_DB_GB") or 17)   # soft pause (ingest waits / stops)
        self.lanes = [x.strip().upper() for x in (os.environ.get("LANES") or "HAIL,ANL,HRRR,OBS,NCEI").split(",")
                      if x.strip()]
        self.lane = (os.environ.get("LANE") or "").strip().upper()
        self.shard = int(os.environ.get("SHARD") or 0)
        self.days_env = (os.environ.get("SHARD_DAYS") or "").strip()
        self.steps, self.notes, self.errors, self.rolled = [], [], [], []
        self.disable_reason = None
        self.status = None
        self._busy_at = 0.0
        self._busy = []
        tag = f"{self.mode}-{self.lane or 'all'}-{self.shard}"
        self.result_path = os.path.join(self.sdir, f"result-{tag}.json")

    # ------------------------------------------------------------ output
    def note(self, msg):
        print(f"[stage5 {self.mode} {self.lane}{self.shard if self.lane else ''}] {msg}", flush=True)
        self.notes.append(msg)

    def error(self, msg):
        print(f"::error::stage5 {self.mode} {self.lane}: {msg}", flush=True)
        self.errors.append(msg)

    def disable(self, reason):
        if self.dry:
            self.note(f"DRY RUN - would DISABLE the workflow: {reason}")
            return
        self.disable_reason = reason
        with open(os.path.join(self.sdir, "DISABLE"), "a") as fh:
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
        for attempt in range(5):
            try:
                code, body = self._rpc(name, payload, self.anon, timeout)
            except Exception as e:
                code, body = None, f"{type(e).__name__}: {e}"
            if code is not None and code < 300:
                return body
            last = f"{name}: HTTP {code} {body}"
            if code in (400, 401, 403, 404):
                break
            time.sleep(3 * (attempt + 1) + random.random() * 3)
        raise RuntimeError(last)

    def gh_get(self, path):
        req = urllib.request.Request(f"https://api.github.com/{path}",
                                     headers={"Authorization": f"Bearer {self.gh}",
                                              "Accept": "application/vnd.github+json",
                                              "X-GitHub-Api-Version": "2022-11-28"})
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())

    def busy(self, max_age=600):
        """Feed workflow runs queued/in progress in the feed repos (cached max_age s)."""
        if time.time() - self._busy_at < max_age:
            return self._busy
        found = []
        for repo, files in FEED_WORKFLOWS.items():
            data = self.gh_get(f"repos/{OWNER}/{repo}/actions/runs?per_page=40")
            for r in data.get("workflow_runs", []):
                if r.get("status") in ("queued", "in_progress", "waiting", "pending", "requested") and \
                        os.path.basename(r.get("path") or "") in files:
                    found.append(f"{repo}/{os.path.basename(r['path'])} run {r['id']} {r['status']}")
        self._busy, self._busy_at = found, time.time()
        return found

    def check_pins(self):
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

    def service_check(self):
        """Proves the service-role key is present and valid without printing it: a table read
        only service_role may do (hz_backfill_log: revoked from anon) + the status RPC."""
        if not self.service:
            return "missing"
        hdr = {"apikey": self.service, "Prefer": "count=exact", "Range": "0-0"}
        if not self.service.startswith("sb_"):
            hdr["Authorization"] = f"Bearer {self.service}"
        req = urllib.request.Request(f"{self.base}/rest/v1/hz_backfill_log?select=id", headers=hdr)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                cr = r.headers.get("Content-Range", "")
            code, body = self._rpc("hz_p5_status_v1", {"p_secret": self.secret}, self.service, 60)
            ok = code == 200 and isinstance(body, dict)
            return f"ok (hz_backfill_log readable, rows {cr.split('/')[-1]}; status rpc {code})" if ok else \
                f"status rpc HTTP {code}"
        except urllib.error.HTTPError as e:
            return f"HTTP {e.code} (key invalid or not service_role)"
        except Exception as e:
            return f"{type(e).__name__}: {e}"

    # ------------------------------------------------------------ DB state
    def refresh_status(self):
        self.status = self.rpc_anon("hz_p5_status_v1", {"p_secret": self.secret})
        return self.status

    def keys(self):
        return (self.status or {}).get("cursors") or {}

    def cursor(self, lane):
        c = self.keys().get(f"p5_{lane.lower()}")
        return dt.date.fromisoformat(c) if c else None

    def next_roll_day(self, lane):
        c = self.cursor(lane)
        return LANES[lane]["ceil"] if not c else c - dt.timedelta(days=1)

    def ready(self, lane, d):
        return self.keys().get(f"p5_ready_{lane.lower()}_{d}") == "ok"

    def set_key(self, key, value):
        if self.dry:
            return
        self.rpc_anon("hz_backfill_set", {"p_secret": self.secret, "p_key": key, "p_value": value})
        if self.status is not None:
            self.status.setdefault("cursors", {})[key] = value

    def fails(self, lane, d):
        try:
            return int(self.keys().get(f"p5_fail_{lane.lower()}_{d}") or 0)
        except ValueError:
            return 0

    def gate_db(self):
        """None = go; 'stop' = stop this job; raises on the size guard."""
        if self.dry and self.status is None:
            return None
        waited = 0
        while True:
            st = self.refresh_status()
            if st["db_bytes"] > self.max_db * 2**30:
                self.disable(f"DB {st['db_bytes'] / 2**30:.2f} GB > {self.max_db} GB guard")
                raise RuntimeError("DB size guard")
            gb = st["db_bytes"] / 2**30
            if st["active"] <= self.max_active and gb <= self.pause_db:
                return None
            if waited >= 600:
                self.note(f"DB busy ({st['active']} active backends, {gb:.2f} GB) for 10 min - stopping this job")
                return "stop"
            w = 30 + random.random() * 60
            self.note(f"DB busy ({st['active']} active > {self.max_active} or {gb:.2f} GB > soft "
                      f"{self.pause_db} GB) - backing off {w:.0f}s")
            time.sleep(w)
            waited += w

    # ------------------------------------------------------------ feed run
    def run_feed(self, lane, dates, extra_env=None):
        cfg = LANES.get(lane) or LANES["OBS"]
        cwd = os.environ[cfg["repo"]]
        tag = f"{lane.lower()}-{(dates or 'none').replace('..', '_').replace(',', '_')}"
        fout = os.path.join(self.out, "feeds", tag)
        os.makedirs(fout, exist_ok=True)
        env = dict(os.environ)
        for k in ("DATE", "INGEST_DATE", "STATES", "TASKS", "NCEI_YEARS", "HOURS_POLICY", "BG_ONLY",
                  "V4_ZONES", "V4_DST", "V4_HRRR_HOUR_ENDING", "COLD_GUARD", "CLEAR_STEP", "REDO_SNAPSHOT",
                  "REDO_RELEASE", "HEAL_SHORT_WINDOWS_FROM", "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_ACCESS_TOKEN",
                  "GH_TOKEN", "GITHUB_TOKEN"):
            env.pop(k, None)
        env.update({"DAY_CONVENTION": "v4", "DRY_RUN": "1" if self.dry else "", "FEED_OUT_DIR": fout,
                    "REDO_RUN_ID": f"{REDO_PREFIX}-{lane.lower()}", "SUPABASE_URL": self.base,
                    "SUPABASE_ANON_KEY": self.anon, "INGEST_SECRET": self.secret})
        if dates:
            env["DATE"] = dates
        env.update(cfg.get("env") or {})
        env.update(extra_env or {})
        t0 = time.time()
        print(f"::group::{lane} {dates or ''} {extra_env or ''}", flush=True)
        try:
            code = subprocess.run([sys.executable, cfg["script"]], cwd=cwd, env=env,
                                  timeout=60 * cfg["timeout"]).returncode
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

    def day_ok(self, lane, res, key):
        if res is None:
            return False, "no FEED_RESULT"
        if lane == "OBS":
            if res.get("status") == "error" or res.get("errors"):
                return False, f"obs errors: {[e.get('msg', '')[:160] for e in res.get('errors', [])][:5]}"
            if not self.dry:
                d = next((x for x in res.get("days", []) if x.get("day") == key), {})
                if (d.get("redo") or {}).get("status") not in ("taken", "exists"):
                    return False, f"redo snapshot missing: {d.get('redo')}"
            return True, "ok"
        d = next((x for x in res.get("days", []) if x.get("day") == key), None)
        if d is None:
            return False, "day missing from FEED_RESULT"
        if d.get("status") not in ("ok", "warning") or d.get("failed"):
            return False, f"status {d.get('status')} failed {d.get('failed')} errors " \
                          f"{[e.get('msg', '')[:160] for e in res.get('errors', [])][:3]}"
        if not self.dry:
            pc = (d.get("redo") or {}).get("postcheck")
            if isinstance(pc, dict) and not pc.get("ok"):
                return False, f"redo post-write counts differ: {pc.get('diffs')}"
            if not isinstance(pc, dict):
                # the feed could not READ the counts (anon 3 s under load): re-check here, with backoff
                ok, why = self.recheck_counts(lane, key, d)
                if not ok:
                    return False, f"redo post-write check: feed said {str(pc)[:120]}; recheck {why}"
                d.setdefault("redo", {})["recheck"] = why
        return True, "ok"

    def recheck_counts(self, lane, key, d):
        rows = d.get("rows") or {}
        exp = {"HAIL": {"hail_points": rows.get("ingest_points.p_points", 0)},
               "ANL": {"wind_points": rows.get("ingest_wind_points.p_points", 0),
                       "hz_station_bg": rows.get("hz_station_bg_ingest.p_rows", 0)},
               "HRRR": {"hz_hrrr_points": rows.get("hz_hrrr_ingest.p_points", 0),
                        "hz_station_bg": rows.get("hz_station_bg_ingest.p_rows", 0)}}[lane]
        last = None
        for attempt in range(6):
            try:
                live = self.rpc_anon("hz_redo_counts", {"p_secret": self.secret, "p_src": lane, "p_date": key})
                diffs = {t: {"live": (live or {}).get(t), "payload": n} for t, n in exp.items()
                         if (live or {}).get(t) != n}
                return (not diffs), ("ok " + json.dumps(exp)) if not diffs else f"differs {diffs}"
            except Exception as e:
                last = str(e)[:160]
                time.sleep(20 * (attempt + 1) + random.random() * 20)
        return False, f"unreadable after retries: {last}"

    @staticmethod
    def summarize_feed(res):
        if not res:
            return None
        days = [{k: d.get(k) for k in ("day", "status", "failed", "rows", "payload_md5", "received")}
                | {"warnings": [w["msg"][:200] for w in res.get("warnings", []) if w.get("day") == d.get("day")],
                   "clear_step": (d.get("clear_step") or {}).get("would_clear", (d.get("clear_step") or {}).get("result")),
                   "redo": {k: (d.get("redo") or {}).get(k) for k in ("status", "postcheck")},
                   "retries": len(d.get("retries") or [])}
                for d in res.get("days", [])]
        meta = res.get("meta") or {}
        return {"feed": res.get("feed"), "status": res.get("status"), "errors": res.get("errors"),
                "write_retries": res.get("write_retries"),
                "meta": {k: meta.get(k) for k in ("day_convention", "zone_map_md5", "hours_policy",
                                                  "hour_ending", "cold_guard", "floor_mph", "tasks")},
                "days": days}

    # ------------------------------------------------------------ roll
    def roll_call(self, lane, day):
        if self.dry:
            return {"ok": True, "status": "dry_run"}
        payload_day = str(day)
        if self.service:
            code, body = self._rpc("hz_p5_roll_v1", {"p_secret": self.secret, "p_lane": lane, "p_day": payload_day,
                                                     "p_run_id": f"{REDO_PREFIX}-{self.run_id}",
                                                     "p_max_db_gb": self.max_db}, self.service, timeout=180)
            if code is None or code >= 300 or not isinstance(body, dict):
                return {"ok": False, "status": "http", "error": f"HTTP {code}: {body}"}
            return body
        if self.mgmt:      # sandbox fallback: management API (runs as postgres; secret read in-DB)
            sql = (f"select hz_p5_roll_v1((select value from app_config where key='ingest_secret'), "
                   f"'{lane}', '{payload_day}'::date, '{REDO_PREFIX}-sandbox', {self.max_db}) r")
            req = urllib.request.Request("https://api.supabase.com/v1/projects/aozjsfjemobuzqrhxcxy/database/query",
                                         data=json.dumps({"query": sql}).encode(),
                                         headers={"Authorization": f"Bearer {self.mgmt}",
                                                  "Content-Type": "application/json", "User-Agent": "curl/8"})
            try:
                with urllib.request.urlopen(req, timeout=180) as r:
                    return json.loads(r.read().decode())[0]["r"]
            except Exception as e:
                return {"ok": False, "status": "http", "error": f"{type(e).__name__}: {e}"}
        return {"ok": False, "status": "nokey", "error": "no SUPABASE_SERVICE_ROLE_KEY"}

    def roll_ready(self, lanes):
        """Roll every contiguous ready day of the lanes (newest first). Returns days rolled."""
        n = 0
        for lane in lanes:
            if lane not in LANES:
                continue
            self.refresh_status()
            while True:
                d = self.next_roll_day(lane)
                if d < FLOOR or not self.ready(lane, d):
                    break
                if minutes_to_forbidden() < 2:
                    return n
                r = self.roll_call(lane, d)
                for attempt in range(6):           # transient: lock/statement timeout, 5xx, network
                    err = str(r.get("error"))
                    if r.get("ok") or r.get("status") != "http" or not any(
                            x in err for x in ("55P03", "57014", "HTTP 5", "HTTP None", "Timeout", "timed out",
                                               "Connection", "URLError")):
                        break
                    w = 10 * (attempt + 1) + random.random() * 10
                    self.note(f"roll {lane} {d}: transient {err[:120]} - retry in {w:.0f}s")
                    time.sleep(w)
                    self.refresh_status()
                    if self.next_roll_day(lane) != d:   # another roller finished it meanwhile
                        r = {"ok": True, "status": "rolled_elsewhere"}
                        break
                    r = self.roll_call(lane, d)
                self.rolled.append({"lane": lane, "day": str(d), "result": r})
                if not r.get("ok"):
                    err = str(r.get("error"))
                    if r.get("status") == "refused" and "is not the next one" in err:
                        self.refresh_status()          # another roller got there first
                        continue
                    if r.get("status") == "http":       # still transient after retries: not the day's fault
                        self.error(f"roll {lane} {d}: transient error persisted ({err[:160]}); next run retries")
                        break
                    k = self.fails(lane, d) + 1
                    self.error(f"roll {lane} {d} {r.get('status')}: {err} (attempt {k} of 3)")
                    self.set_key(f"p5_fail_{lane.lower()}_{d}", str(k))
                    self.set_key(f"p5_issue_{lane.lower()}_{d}", json.dumps(
                        {"why": f"roll {r.get('status')}: {err[:250]}", "attempts": k, "run": self.run_id,
                         "at": now().isoformat()[:16]}))
                    self.set_key(f"p5_ready_{lane.lower()}_{d}", "")     # re-ingest it
                    if k >= 3:
                        self.disable(f"roll {lane} {d} failed 3 times: {err[:200]}")
                    break
                if r.get("status") == "rolled_elsewhere":
                    continue
                n += 1
                if r.get("status") == "rolled_warn":
                    print(f"::warning::stage5 {lane} {d}: row count outside 0.2x-5x of the recent median "
                          f"{r.get('warnings')}", flush=True)
                self.set_key(f"p5_{lane.lower()}", str(d))
        return n

    # ------------------------------------------------------------ plan
    def pending_days(self, lane, limit):
        out = []
        start = self.next_roll_day(lane)
        d = start
        while d >= FLOOR and len(out) < limit and (start - d).days < self.lookahead:
            if not self.ready(lane, d):
                out.append(d)
            d -= dt.timedelta(days=1)
        return out

    def plan(self):
        shards = shard_counts()
        include = []
        if self.dry and os.environ.get("DATES"):
            days = sorted({x.strip() for x in os.environ["DATES"].split(",") if x.strip()}, reverse=True)
            for lane in self.lanes:
                if lane == "NCEI":
                    continue
                for s in range(shards[lane]):
                    mine = days[s::shards[lane]]
                    if mine:
                        include.append({"lane": lane, "shard": s, "days": ",".join(mine)})
            if "NCEI" in self.lanes and not any(i["lane"] == "OBS" for i in include):
                include.append({"lane": "OBS", "shard": 0, "days": ""})
            return include, "dry-run sample dates"
        mtf = minutes_to_forbidden()
        budget = min(self.budget_s / 60, mtf - 5)
        if budget < 10:
            return [], f"{mtf:.0f} min to a forbidden window - no work this run"
        busy = self.busy(0)
        if busy:
            return [], f"feed workflows active: {busy}"
        if not self.dry and not self.service:
            self.disable("SUPABASE_SERVICE_ROLE_KEY secret missing")
            return [], "no service key"
        for lane in LANE_ORDER:
            if lane not in self.lanes:
                continue
            per_shard = max(1, int(budget // LANES[lane]["per_day"]))
            days = self.pending_days(lane, per_shard * shards[lane])
            for s in range(shards[lane]):
                mine = [str(d) for d in days[s::shards[lane]]]
                if mine:
                    include.append({"lane": lane, "shard": s, "days": ",".join(mine)})
        ncei_c = self.keys().get("p5_ncei")
        if "NCEI" in self.lanes and (not ncei_c or int(ncei_c[:4]) > NCEI_YEARS[-1]):
            if not any(i["lane"] == "OBS" and i["shard"] == 0 for i in include):
                include.append({"lane": "OBS", "shard": 0, "days": ""})
        done = all(self.next_roll_day(l) < FLOOR for l in LANE_ORDER) and ncei_c and int(ncei_c[:4]) <= NCEI_YEARS[-1]
        if done and set(self.lanes) >= set(LANE_ORDER) | {"NCEI"}:
            self.note("STAGE 5 COMPLETE: every lane reached 2021-10-01 and NCEI 2021-2023 is loaded")
            self.disable("Stage 5 backfill complete")
        return include, f"budget {budget:.0f} min"

    # ------------------------------------------------------------ shard
    def ncei(self):
        c = self.keys().get("p5_ncei")
        years = [y for y in NCEI_YEARS if not c or y < int(c[:4])] if not (self.dry and os.environ.get("DATES")) \
            else sorted({int(x.strip()[:4]) for x in os.environ["DATES"].split(",") if x.strip()}, reverse=True)
        for y in years:
            code, res, secs = self.run_feed("NCEI", "", {"TASKS": "ncei", "NCEI_YEARS": str(y)})
            st = {"lane": "NCEI", "day": str(y), "exit": code, "seconds": secs, "feed": self.summarize_feed(res)}
            self.steps.append(st)
            d = next((x for x in (res or {}).get("days", []) if x.get("day") == f"ncei {y}"), {})
            if res is None or res.get("errors") or set(d.get("written") or []) != {"hz_storm_events", "hz_storm_events_v4"}:
                self.error(f"NCEI {y} failed: {(res or {}).get('errors')}")
                return
            r = self.roll_call("NCEI", dt.date(y, 1, 1))
            st["roll"] = r
            if not r.get("ok"):
                self.error(f"NCEI {y} roll {r.get('status')}: {r.get('error')}")
                return

    def wait_for_roll(self, lane, d):
        """HARD cap: day d may be ingested only while it is < MAX_AHEAD days ahead of the
        lane's next roll day. Otherwise roll what is ready and wait (<= 15 min), else False."""
        waited = 0
        while True:
            if self.dry:
                return True
            self.refresh_status()
            ahead = (self.next_roll_day(lane) - d).days
            if ahead < self.max_ahead:
                return True
            self.roll_ready([lane])
            if (self.next_roll_day(lane) - d).days < self.max_ahead:
                return True
            if waited >= 900 or minutes_to_forbidden() < 5:
                nr = self.next_roll_day(lane)
                self.note(f"{lane} roll is held at {nr} (fail count {self.fails(lane, nr)}); {d} would be "
                          f"{ahead} days ahead (cap {self.max_ahead}) - lane stops ingesting this run")
                return False
            time.sleep(45 + random.random() * 30)
            waited += 60

    def run_shard(self):
        lane = self.lane
        cfg = LANES[lane]
        days = [dt.date.fromisoformat(x) for x in self.days_env.split(",") if x.strip()]
        days.sort(reverse=True)
        time.sleep(self.shard * 7 + random.random() * 5)          # stagger the shards' upstream hits
        if lane == "OBS" and self.shard == 0 and "NCEI" in self.lanes:
            self.ncei()
        queue = list(days)
        while queue:
            d = queue.pop(0)
            if (now() - self.started).total_seconds() > self.budget_s:
                self.note("time budget used up"); break
            if minutes_to_forbidden() < cfg["per_day"] + 3:
                self.note("a forbidden window starts soon - stopping (frees the runner)"); break
            if not self.dry:
                try:
                    busy = self.busy()
                except Exception as e:
                    busy = [f"busy check failed: {e}"]
                if busy:
                    self.note(f"yielding to feed workflows: {busy}"); break
                if self.gate_db() == "stop":
                    break
                if self.ready(lane, d) or (self.cursor(lane) and d >= self.cursor(lane)):
                    continue
                if not self.wait_for_roll(lane, d):
                    break
            extra = None
            if lane == "OBS":
                extra = {"TASKS": "dailies,peaks" if d >= LSR_EXISTS_FROM else "dailies,peaks,lsr"}
            code, res, secs = self.run_feed(lane, str(d), extra)
            ok, why = self.day_ok(lane, res, str(d))
            self.steps.append({"lane": lane, "day": str(d), "exit": code, "seconds": secs, "ok": ok, "why": why,
                               "feed": self.summarize_feed(res)})
            if not ok:
                k = self.fails(lane, d) + 1
                self.error(f"{lane} {d}: {why} (attempt {k} of 3 for this day)")
                if not self.dry:
                    self.set_key(f"p5_fail_{lane.lower()}_{d}", str(k))
                    self.set_key(f"p5_issue_{lane.lower()}_{d}", json.dumps(
                        {"why": why[:300], "attempts": k, "run": self.run_id, "at": now().isoformat()[:16]}))
                    if k >= 3:
                        self.disable(f"{lane} {d} failed 3 times: {why[:200]}")
                        break
                    if d == self.next_roll_day(lane):        # the lane's roll blocker: retry it in this run
                        w = 120 + random.random() * 60
                        self.note(f"{d} blocks the {lane} roll - retrying in {w:.0f}s")
                        time.sleep(w)
                        queue.insert(0, d)
                continue
            self.set_key(f"p5_ready_{lane.lower()}_{d}", "ok")
            if not self.dry and self.keys().get(f"p5_fail_{lane.lower()}_{d}"):
                self.set_key(f"p5_issue_{lane.lower()}_{d}", json.dumps(
                    {"why": "resolved", "attempts": self.fails(lane, d), "run": self.run_id, "at": now().isoformat()[:16]}))
            retries = (res or {}).get("write_retries") or 0
            if retries:
                w = min(300, 30 * retries) + random.random() * 30
                self.note(f"{retries} write retries (DB pressure) - pausing {w:.0f}s")
                time.sleep(w)
            if not self.dry and self.shard == 0:      # one roller per lane (others roll only while waiting on the cap)
                self.roll_ready([lane])

    # ------------------------------------------------------------ progress
    def svc_get(self, path):
        if not self.service:
            return None
        hdr = {"apikey": self.service}
        if not self.service.startswith("sb_"):
            hdr["Authorization"] = f"Bearer {self.service}"
        try:
            with urllib.request.urlopen(urllib.request.Request(f"{self.base}/rest/v1/{path}", headers=hdr),
                                        timeout=60) as r:
                return json.loads(r.read().decode())
        except Exception:
            return None

    def progress(self):
        """Per-lane status (no secrets): rolled-through, remaining, raw in flight, issues, rate, ETA."""
        self.refresh_status()
        since = (now() - dt.timedelta(hours=2)).isoformat()
        log = self.svc_get(f"hz_backfill_log?select=lane,src,day,status,exact,run_at&run_at=gte.{since}") or []
        keys = self.keys()
        out = {"at": now().isoformat()[:16] + "Z", "db_gb": round(self.status["db_bytes"] / 2**30, 2),
               "active": self.status["active"], "lanes": {}}
        for lane in LANE_ORDER:
            cur = self.cursor(lane)
            nxt = self.next_roll_day(lane)
            remaining = max(0, (nxt - FLOOR).days + 1)
            done = (LANES[lane]["ceil"] - nxt).days
            inflight = sum(1 for k, v in keys.items() if k.startswith(f"p5_ready_{lane.lower()}_") and v == "ok"
                           and dt.date.fromisoformat(k[-10:]) <= nxt)
            issues = []
            for k, v in keys.items():
                if k.startswith(f"p5_issue_{lane.lower()}_") and v:
                    day = dt.date.fromisoformat(k[-10:])
                    try:
                        info = json.loads(v)
                    except ValueError:
                        info = {"why": v}
                    info["status"] = "resolved (rolled)" if day > nxt else info.get("why") == "resolved" and \
                        "resolved (ingested)" or "OPEN"
                    issues.append({"day": str(day), **info})
            rolled2h = [r for r in log if r.get("lane") == lane and r.get("status") in ("rolled", "rolled_warn", "obs_ok")
                        and r.get("src") in (LANES[lane]["srcs"][:1] or [None])]
            nonexact = [r for r in log if r.get("lane") == lane and r.get("status") in ("rolled", "rolled_warn")
                        and r.get("exact") is not True]
            rate = len(rolled2h) / 2.0
            out["lanes"][lane] = {"rolled_through": str(cur) if cur else None, "next": str(nxt), "done": done,
                                  "remaining": remaining, "raw_in_flight_days": inflight,
                                  "rolled_per_hour_2h": rate, "eta_h": round(remaining / rate, 1) if rate else None,
                                  "non_exact_2h": len(nonexact), "issues": sorted(issues, key=lambda x: x["day"])}
        c = keys.get("p5_ncei")
        out["ncei_done_through"] = c[:4] if c else None
        with open(os.path.join(self.sdir, "progress.json"), "w") as fh:
            json.dump(out, fh, indent=1)
        sp = os.environ.get("GITHUB_STEP_SUMMARY")
        if sp:
            with open(sp, "a") as fh:
                fh.write(f"### Stage 5 progress {out['at']} - DB {out['db_gb']} GB, {out['active']} active backends, "
                         f"NCEI through {out['ncei_done_through']}\n\n| lane | rolled through | done | remaining | "
                         f"raw in flight (days) | rolled/h (2h) | ETA h | non-exact | open issues |\n|---|---|---|---|---|---|---|---|---|\n")
                for lane, x in out["lanes"].items():
                    op = [f"{i['day']}: {str(i.get('why'))[:80]} (x{i.get('attempts')})" for i in x["issues"] if i["status"] == "OPEN"]
                    fh.write(f"| {lane} | {x['rolled_through']} | {x['done']} | {x['remaining']} | {x['raw_in_flight_days']} | "
                             f"{x['rolled_per_hour_2h']} | {x['eta_h']} | {x['non_exact_2h']} | {'; '.join(op) or '-'} |\n")
        print("STAGE5_PROGRESS " + json.dumps(out, separators=(",", ":")), flush=True)
        return out

    # ------------------------------------------------------------ main
    def main(self):
        self.note(f"{'DRY RUN (nothing is written)' if self.dry else 'LIVE'}; mode {self.mode}")
        if self.mode in ("plan", "shard"):
            self.check_pins() if self.mode == "shard" else None
        if not self.dry and (os.environ.get("DATES") or flag("IGNORE_WINDOW")):
            raise RuntimeError("DATES / IGNORE_WINDOW are dry-run-only inputs")
        try:
            self.refresh_status()
        except Exception as e:
            if self.dry:
                self.note(f"hz_p5_status_v1 unavailable: {str(e)[:160]}")
            else:
                if "HTTP 404" in str(e):
                    self.disable("hz_p5_status_v1 missing")
                raise
        if self.mode == "plan":
            svc = self.service_check()
            self.note(f"service-role key: {svc}")
            if not self.dry and not svc.startswith("ok"):
                self.disable(f"service-role key not usable: {svc}")
                raise RuntimeError("service-role key")
            if self.status:
                self.note(f"DB {self.status['db_bytes'] / 2**30:.2f} GB, active {self.status['active']}, "
                          f"archive starts {self.status.get('arch_min')}, cursors "
                          f"{ {k: v for k, v in self.keys().items() if not k.startswith(('p5_ready', 'p5_fail'))} }")
            try:
                busy = self.busy(0)
                self.note(f"feed workflows active: {busy or 'none'}")
            except Exception as e:
                self.note(f"busy check failed: {e}")
            try:
                self.progress()
            except Exception as e:
                self.note(f"progress unavailable: {e}")
            include, why = self.plan()
            self.note(f"plan: {len(include)} shard job(s) ({why})")
            mat = json.dumps({"include": include or [{"lane": "NONE", "shard": 0, "days": ""}]})
            go = "true" if include else "false"
            gho = os.environ.get("GITHUB_OUTPUT")
            if gho:
                with open(gho, "a") as fh:
                    fh.write(f"matrix={mat}\ngo={go}\n")
            print("PLAN " + mat, flush=True)
            return 0
        if self.mode == "shard":
            if self.lane == "NONE":
                return 0
            self.run_shard()
            return 1 if self.errors else 0
        if self.mode == "roll":
            if self.dry:
                self.note("DRY RUN - roll step reads the state only")
                for lane in LANE_ORDER:
                    if self.status:
                        self.note(f"{lane}: next roll day {self.next_roll_day(lane)}, ready {self.ready(lane, self.next_roll_day(lane))}")
                return 0
            n = self.roll_ready([l for l in LANE_ORDER if l in self.lanes])
            self.note(f"rolled {n} day(s)")
            self.progress()
            return 1 if self.errors else 0
        raise RuntimeError(f"unknown MODE {self.mode}")

    def finish(self, code):
        res = {"status": "error" if (code or self.errors) else "ok", "dry_run": self.dry, "mode": self.mode,
               "lane": self.lane, "shard": self.shard, "run_id": self.run_id,
               "started_utc": self.started.isoformat(), "finished_utc": now().isoformat(),
               "steps": self.steps, "rolled": self.rolled, "notes": self.notes, "errors": self.errors,
               "disable": self.disable_reason}
        with open(self.result_path, "w") as fh:
            json.dump(res, fh, indent=1, default=str)
        sp = os.environ.get("GITHUB_STEP_SUMMARY")
        if sp:
            with open(sp, "a") as fh:
                fh.write(f"### Stage 5 {self.mode} {self.lane} {self.shard}: {res['status']}"
                         f"{' (DRY RUN)' if self.dry else ''}\n\n| lane | day | s | ok | why |\n|---|---|---|---|---|\n")
                for s in self.steps:
                    fh.write(f"| {s['lane']} | {s['day']} | {s['seconds']} | {s.get('ok')} | {str(s.get('why') or '')[:120]} |\n")
                fh.write(f"\nrolled: {[(r['lane'], r['day'], r['result'].get('status')) for r in self.rolled]}\n")
                for n in self.notes + [f"**error** {e}" for e in self.errors]:
                    fh.write(f"- {n}\n")
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
