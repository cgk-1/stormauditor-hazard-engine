"""Stage 6 STAGING driver (TEMPORARY; owner-approved plan 2026-10-09 "S1 Finish staging", recorded in
stormwatch-insight CLAUDE.md AUTOMATION RULES and docs/STAGE6-FINISH-PLAN-2026-10-09.md).

Builds the correctly dated (v4) copy of the middle stretch OFF TO THE SIDE: each day runs a feed's OWN
entrypoint with DRY_RUN=1 (every write call is saved to FEED_OUT_DIR/payloads instead of being sent), and
the saved calls are loaded into the staging table hz_s6_payload via the secret-gated RPC hz_s6_load.
Nothing here writes to a live table; switching a day is a separate, owner-approved step.

MODE=plan   -> remaining days per lane (range minus already staged), matrix for the shard jobs
MODE=shard  -> stage this shard's days (LANE, SHARD, SHARDS)
MODE=chain  -> dispatch the next run if days remain AND this run made progress (never loops without progress)
Never works 05:15-05:45, 09:00-11:00, 11:50-12:30, 15:50-17:00 UTC (feeds + nightly archive roll).
D5: a winter hail day whose cold-season IEM check could not run is not loaded (retried next run).
"""
import datetime as dt, gzip, json, os, shutil, subprocess, sys, time, urllib.request, urllib.error

UTC = dt.timezone.utc
RANGES = {"HAIL": ("2023-09-28", "2026-10-06"), "ANL": ("2024-07-23", "2026-08-28"), "HRRR": ("2024-07-25", "2026-10-06")}
SHARDS = {"HAIL": 4, "ANL": 10, "HRRR": 6}
FEEDS = {"HAIL": ("HAIL_DIR", "mesh_ingest.py", "hail"), "ANL": ("WIND_DIR", "wind_ingest.py", "wind"),
         "HRRR": ("HAZARD_DIR", "hz_hrrr_ingest.py", "hrrr")}
FORBID = [(5 * 60 + 15, 5 * 60 + 45), (9 * 60, 11 * 60), (11 * 60 + 50, 12 * 60 + 30), (15 * 60 + 50, 17 * 60)]
URL = os.environ.get("SUPABASE_URL", "https://aozjsfjemobuzqrhxcxy.supabase.co")
KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
SECRET = os.environ.get("INGEST_SECRET", "")
OUT = os.environ.get("OUT_DIR", "feed-out")
BUDGET_MIN = float(os.environ.get("TIME_BUDGET_MIN", "320"))
T0 = time.time()


def log(*a):
    print(dt.datetime.now(UTC).strftime("%H:%M:%SZ"), *a, flush=True)


def rest(method, path, body=None, headers=None, timeout=180):
    h = {"apikey": KEY, "Authorization": "Bearer " + KEY, "Content-Type": "application/json"}
    h.update(headers or {})
    data = json.dumps(body).encode() if body is not None else None
    for a in range(6):
        try:
            req = urllib.request.Request(URL + path, data=data, method=method, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                txt = r.read().decode()
                return json.loads(txt) if txt else None
        except urllib.error.HTTPError as e:
            msg = e.read().decode()[:300]
            if e.code >= 500 or e.code in (408, 429) or "57014" in msg:
                time.sleep(5 * (a + 1)); continue
            raise RuntimeError(f"{method} {path.split('?')[0]} -> {e.code} {msg}")
        except Exception as e:  # network
            time.sleep(5 * (a + 1)); last = e
    raise RuntimeError(f"{method} {path.split('?')[0]} failed after retries")


def staged_days(lane):
    out, off = set(), 0
    while True:
        rows = rest("GET", f"/rest/v1/hz_s6_day?select=day&lane=eq.{lane}&status=in.(staged,dry_ok,switched)"
                           f"&result=not.is.null&order=day&limit=1000&offset={off}")
        out |= {r["day"] for r in rows}
        if len(rows) < 1000: return out
        off += 1000


def remaining(lane):
    a, b = (dt.date.fromisoformat(x) for x in RANGES[lane])
    done = staged_days(lane)
    return [str(b - dt.timedelta(d)) for d in range((b - a).days + 1) if str(b - dt.timedelta(d)) not in done]


def in_window(lead=10):
    t = dt.datetime.now(UTC); m = t.hour * 60 + t.minute
    return any(s - lead <= m < e for s, e in FORBID)


def load(lane, day, pfile, dayres):
    lines = [json.loads(l) for l in gzip.open(pfile, "rt")] if os.path.exists(pfile) else []
    first, chunk, size = 0, [], 0
    def flush(final):
        nonlocal first, chunk
        rest("POST", "/rest/v1/rpc/hz_s6_load", {"p_secret": SECRET, "p_lane": lane, "p_day": day, "p_first": first,
             "p_lines": chunk, "p_result": dayres if final else None, "p_max_db_gb": 40})
        first += len(chunk); chunk = []
    for l in lines:
        chunk.append({"scope": l.get("scope"), "rpc": l["rpc"], "payload": l["payload"]})
        size += len(json.dumps(l["payload"]))
        if size > 700_000: flush(False); size = 0
    flush(True)
    return len(lines)


def stage_day(lane, day):
    env_dir, script, tag = FEEDS[lane]
    feed_dir = os.environ[env_dir]
    out = os.path.join(OUT, f"{lane}-{day}")
    shutil.rmtree(out, ignore_errors=True); os.makedirs(out)
    env = dict(os.environ, DATE=day, INGEST_DATE=day, DAY_CONVENTION="v4", DRY_RUN="1", FEED_OUT_DIR=out,
               STATE_PAUSE="0", INGEST_SECRET=SECRET)
    env.pop("STATES", None)
    if lane == "HRRR": env["FLOOR_MPH"] = "35"          # same as daily-new.yml
    p = subprocess.run([sys.executable, script], cwd=feed_dir, env=env, capture_output=True, text=True, timeout=3600)
    try:
        res = json.load(open(os.path.join(out, "result.json")))
    except Exception:
        log(lane, day, "NO RESULT", p.returncode, (p.stdout + p.stderr)[-600:].replace("\n", " | ")); return "fail"
    dres = next((d for d in res.get("days", []) if d.get("day") == day), None)
    if p.returncode != 0 or dres is None or dres.get("failed"):
        log(lane, day, "FEED FAIL", p.returncode, json.dumps(dres)[:600] if dres else (p.stdout + p.stderr)[-600:]); return "fail"
    if lane == "HAIL" and "cold-season guard could not check" in json.dumps(dres.get("warnings", [])):
        log(lane, day, "D5: cold-season IEM check failed -> not loaded (next run)"); return "retry"
    n = load(lane, day, os.path.join(out, "payloads", f"{tag}-{day}.jsonl.gz"), dres)
    shutil.rmtree(out, ignore_errors=True)
    return f"ok {n}"


def main():
    mode = os.environ.get("MODE", "plan")
    os.makedirs(OUT, exist_ok=True)
    if mode == "plan":
        lanes = [l for l in (os.environ.get("LANES") or "HAIL,ANL,HRRR").split(",") if l in RANGES]
        rem = {l: len(remaining(l)) for l in lanes}
        log("remaining days:", rem)
        json.dump(rem, open(os.path.join(OUT, "remaining.json"), "w"))
        mx = [{"lane": l, "shard": i, "shards": SHARDS[l]} for l in lanes if rem[l] for i in range(SHARDS[l])]
        with open(os.environ["GITHUB_OUTPUT"], "a") as fh:
            fh.write(f"matrix={json.dumps({'include': mx})}\n")
            fh.write(f"go={'true' if mx else 'false'}\n")
            fh.write(f"remaining={sum(rem.values())}\n")
        if not mx:
            open(os.path.join(OUT, "COMPLETE"), "w").write("all lanes staged")
        return
    if mode == "shard":
        lane, shard, shards = os.environ["LANE"], int(os.environ["SHARD"]), int(os.environ["SHARDS"])
        days = [d for i, d in enumerate(remaining(lane)) if i % shards == shard]
        log(f"{lane} shard {shard}/{shards}: {len(days)} days")
        ok = bad = 0
        for d in days:
            if (time.time() - T0) / 60 > BUDGET_MIN: log("time budget reached"); break
            while in_window():
                log("forbidden window: waiting"); time.sleep(120)
                if (time.time() - T0) / 60 > BUDGET_MIN: break
            st = stage_day(lane, d)
            log(lane, d, st)
            if st.startswith("ok"): ok += 1
            else: bad += 1
        log(f"done: {ok} staged, {bad} not")
        return
    if mode == "chain":
        before = int(os.environ.get("REMAINING_BEFORE", "0") or 0)
        after = sum(len(remaining(l)) for l in RANGES)
        log(f"remaining before {before}, after {after}")
        if after == 0 or after >= before:
            log("no chain (complete or no progress)"); return
        while in_window(lead=15): time.sleep(120)
        subprocess.run(["gh", "workflow", "run", "stage6-stage.yml", "--repo", os.environ["GITHUB_REPOSITORY"]], check=True)
        log("dispatched next run")


if __name__ == "__main__":
    main()
