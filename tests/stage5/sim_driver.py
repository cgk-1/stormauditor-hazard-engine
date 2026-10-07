"""Logic simulation of stage5_backfill.Driver with a fake DB + fake feeds (no network)."""
import datetime as dt, json, os, sys, importlib
sys.path.insert(0, sys.argv[1])
os.environ.update({"HAIL_DIR": sys.argv[2], "WIND_DIR": sys.argv[3], "HAZARD_DIR": sys.argv[1],
                   "OUT_DIR": sys.argv[4], "SUPABASE_URL": "x", "SUPABASE_ANON_KEY": "a",
                   "SUPABASE_SERVICE_ROLE_KEY": "svc", "INGEST_SECRET": "s", "TIME_BUDGET_MIN": "120", "RUN_MODE": "live"})
import stage5_backfill as S
class FakeDB:
    def __init__(s):
        s.arch_min = {"HAIL": "2023-09-28", "ANL": "2024-07-24", "BGA": "2024-07-23", "HRRR": "2024-07-25", "BGH": "2024-07-25"}
        s.cur = {}; s.db = 6.6 * 2**30; s.rolls = []
    def status(s):
        return {"db_bytes": s.db, "active": 1, "arch_min": dict(s.arch_min), "cursors": dict(s.cur)}
    def roll(s, lane, day):
        day = dt.date.fromisoformat(day)
        for src in S.LANES.get(lane, {"srcs": []})["srcs"]:
            if dt.date.fromisoformat(s.arch_min[src]) != day + dt.timedelta(days=1):
                return {"ok": False, "status": "refused", "error": f"{src} not contiguous"}
        for src in S.LANES.get(lane, {"srcs": []})["srcs"]:
            s.arch_min[src] = str(day)
        s.cur["p5_" + lane.lower()] = str(day)
        s.rolls.append((lane, str(day)))
        return {"ok": True, "status": "ok"}
db = FakeDB()
FAIL = set(sys.argv[5].split(",")) if len(sys.argv) > 5 else set()
def run(clock, lanes="HAIL,ANL,HRRR,OBS,NCEI"):
    os.environ["LANES"] = lanes
    d = S.Driver()
    d.busy = lambda: []
    S.now = lambda: clock
    d.started = clock
    def rpc_anon(name, payload, timeout=60):
        if name == "hz_p5_status_v1": return db.status()
        if name == "hz_backfill_set": db.cur[payload["p_key"]] = payload["p_value"]; return None
        raise RuntimeError(name)
    d.rpc_anon = rpc_anon
    def _rpc(name, payload, key, timeout=120):
        assert key == "svc"
        return 200, db.roll(payload["p_lane"], payload["p_day"])
    d._rpc = _rpc
    def run_feed(lane, dates, extra=None):
        if lane == "NCEI":
            y = extra["NCEI_YEARS"]
            return 0, {"status": "ok", "errors": [], "days": [{"day": f"ncei {y}", "status": "ok", "failed": [], "written": ["hz_storm_events", "hz_storm_events_v4"]}]}, 1
        if ".." in dates:
            a, b = [dt.date.fromisoformat(x) for x in dates.split("..")]
        else:
            a = b = dt.date.fromisoformat(dates)
        days = []
        x = a
        while x <= b:
            bad = f"{lane}:{x}" in FAIL
            days.append({"day": str(x), "status": "error" if bad else "ok", "failed": ["TX"] if bad else [],
                         "redo": {"status": "taken", "postcheck": {"ok": True}}})
            x += dt.timedelta(days=1)
        err = [{"msg": "boom"}] if any(dd["status"] == "error" for dd in days) else []
        d.calls.append((lane, dates, (extra or {}).get("TASKS")))
        return (1 if err else 0), {"status": "error" if err else "ok", "errors": err, "days": days}, 1
    d.calls = []
    d.run_feed = run_feed
    rc = d.main()
    rc = d.finish(rc)
    return rc, d
# 1: outside window
rc, d = run(dt.datetime(2026, 10, 8, 10, 0, tzinfo=S.UTC)); print("10:00Z ->", rc, d.calls, d.notes[-1][:60])
# 2: 00:05Z run with 120 min budget but window closes 05:15
rc, d = run(dt.datetime(2026, 10, 8, 0, 5, tzinfo=S.UTC)); print("00:05Z ->", rc, d.calls[:8], "...", len(d.calls), "calls")
print("rolls so far", len(db.rolls), db.rolls[:6], "cursors", db.cur)
# 3: allow ANL by listing 2024-07-23
db.arch_min["ANL"] = "2024-07-23"
rc, d = run(dt.datetime(2026, 10, 8, 18, 0, tzinfo=S.UTC)); print("18:00Z ->", rc, d.calls[:10], "warn", [n for n in d.notes][-2:])
print("cursors", db.cur, "arch_min", db.arch_min)
