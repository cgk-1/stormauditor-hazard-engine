"""Logic simulation of the parallel driver (plan + roll_ready) with a fake DB; no network."""
import datetime as dt, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
os.environ.update({"RUN_MODE": "live", "SUPABASE_SERVICE_ROLE_KEY": "svc", "INGEST_SECRET": "s",
                   "SUPABASE_URL": "x", "SUPABASE_ANON_KEY": "a", "OUT_DIR": "/tmp/s5sim"})
import stage5_backfill as S
keys = {}
arch = {"HAIL": "2023-09-28", "ANL": "2024-07-23", "BGA": "2024-07-23", "HRRR": "2024-07-25", "BGH": "2024-07-25"}
def status():
    return {"db_bytes": 7 * 2**30, "active": 2, "arch_min": dict(arch), "cursors": dict(keys)}
def mk(mode, **env):
    os.environ["MODE"] = mode
    for k, v in env.items(): os.environ[k] = v
    d = S.Driver(); d.busy = lambda max_age=600: []
    d.rpc_anon = lambda n, p, timeout=60: status() if n == "hz_p5_status_v1" else keys.__setitem__(p["p_key"], p["p_value"])
    def roll(lane, day):
        day = dt.date.fromisoformat(str(day)); c = keys.get(f"p5_{lane.lower()}")
        nxt = S.LANES[lane]["ceil"] if not c else dt.date.fromisoformat(c) - dt.timedelta(days=1)
        if day != nxt: return {"ok": False, "status": "refused", "error": "is not the next one"}
        for s in S.LANES[lane]["srcs"]:
            if dt.date.fromisoformat(arch[s]) != day + dt.timedelta(days=1): return {"ok": False, "status": "refused", "error": "hole"}
            arch[s] = str(day)
        keys[f"p5_{lane.lower()}"] = str(day); return {"ok": True, "status": "ok"}
    d.roll_call = roll
    return d
S.minutes_to_forbidden = lambda t=None: 200
d = mk("plan"); d.refresh_status(); inc, why = d.plan()
by = {}
for i in inc: by.setdefault(i["lane"], []).append(len(i["days"].split(",")) if i["days"] else 0)
print("plan:", why, by, "first ANL shards:", [i["days"][:32] for i in inc if i["lane"] == "ANL"][:3])
# mark ANL days ready out of order: shard 1's days ready, shard 0's first missing -> no roll
anl = [i for i in inc if i["lane"] == "ANL"]
for dd in anl[1]["days"].split(","): keys[f"p5_ready_anl_{dd}"] = "ok"
d = mk("roll"); print("roll with a gap:", d.roll_ready(["ANL"]), keys.get("p5_anl"))
for dd in anl[0]["days"].split(",")[:3]: keys[f"p5_ready_anl_{dd}"] = "ok"
d = mk("roll"); print("roll after gap filled:", d.roll_ready(["ANL"]), keys.get("p5_anl"), arch["ANL"], arch["BGA"])
d = mk("plan"); d.refresh_status(); inc2, _ = d.plan()
print("next plan ANL first days:", sorted({x for i in inc2 if i['lane']=='ANL' for x in i['days'].split(',')}, reverse=True)[:4])
