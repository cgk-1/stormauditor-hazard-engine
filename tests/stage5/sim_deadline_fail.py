import datetime as dt, os, sys
exec(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "sim_driver.py")).read().split("# 1: outside window")[0])
clock = [dt.datetime(2026, 10, 8, 22, 40, tzinfo=S.UTC)]
S.now = lambda: clock[0]
orig_run = run
def run2(c0, lanes="HAIL,ANL,HRRR,OBS,NCEI"):
    clock[0] = c0
    os.environ["LANES"] = lanes
    d = S.Driver(); d.busy = lambda: []; d.started = c0
    d.rpc_anon = lambda n, p, timeout=60: db.status() if n == "hz_p5_status_v1" else db.cur.__setitem__(p["p_key"], p["p_value"])
    d._rpc = lambda n, p, k, timeout=120: (200, db.roll(p["p_lane"], p["p_day"]))
    def rf(lane, dates, extra=None):
        a, b = ([dt.date.fromisoformat(x) for x in dates.split("..")] if ".." in dates else [dt.date.fromisoformat(dates)] * 2)
        n = (b - a).days + 1
        clock[0] += dt.timedelta(minutes=S.LANES[lane]["per_day"] * n * 0.7)
        days = [{"day": str(a + dt.timedelta(days=i)), "status": "error" if f"{lane}:{a + dt.timedelta(days=i)}" in FAIL else "ok",
                 "failed": ["x"] if f"{lane}:{a + dt.timedelta(days=i)}" in FAIL else [], "redo": {"status": "taken", "postcheck": {"ok": True}}} for i in range(n)]
        err = [{"msg": "boom"}] if any(x["status"] == "error" for x in days) else []
        d.calls.append((lane, dates, clock[0].strftime("%H:%M")))
        return 0, {"status": "error" if err else "ok", "errors": err, "days": days}, 1
    d.calls = []; d.run_feed = rf
    rc = d.finish(d.main()); return rc, d
db.cur["p5_ncei"] = "2021-01-01"
rc, d = run2(dt.datetime(2026, 10, 8, 22, 40, tzinfo=S.UTC)); print("22:40 ->", rc, d.calls)
for i in range(3):
    rc, d = run2(dt.datetime(2026, 10, 9, 0, 5, tzinfo=S.UTC), "HRRR"); print("fail run", i, rc, d.calls, d.errors, d.disable_reason, db.cur.get("p5_fail"))
