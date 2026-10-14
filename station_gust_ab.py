#!/usr/bin/env python3
"""
StormAuditor HAZARD ENGINE - nightly station-gust rules A/B (owner-approved 2026-10-14, "approve" of
docs/qc/rule-ab-preview-2026-10-14.md in the site repo). Runs right after station_gust_qc.py (v5.2) in the
obs ingest; like it, any failure here is a WARNING, never an ingest failure.

  Rule A  - proven-bad stations (hz_gust_suspect_station + 5+ confirmed v5.2 sensor faults), 58-99 mph: kept only
            if the value is in the station's own METAR/SPECI (within 2 mph) AND it has backup: a normal station
            within 50 mi read >= 60% of it; an independent NWS wind report within 15 mi; the station's own thunder
            plus a storm report within 50 mi (or a station within 100 mi at 45+); or a hurricane within 150 mi.
  Rule A2 - glitch-prone stations (hz_gust_qc_glitch_station), 58-99 mph: removed only when it is an obvious
            error: in its own reports, no backup as in A, and every reporting normal station within 50 mi read
            under half of it.
  Rule B  - every reading >= 100 mph: kept with a hurricane within 150 mi; else removed if > 130 mph, not in its own
            reports, or without backup (station within 75 mi >= 80%, 2 stations within 100 mi at 75+, measured
            NWS report >= 75% within 25 mi). HELD (warned, not moved) instead when the date has no published
            hurricane tracks yet and 2+ stations within 100 mi read 75+, or on a storm day (tornado / tropical /
            3+ wind reports / Storm Data wind nearby) at a station with no glitch record.
  Grid    - never removed when the HRRR grid at the station read >= 58 mph and >= 60% of it and the station's own
            reports show the value (meteorology reviewer 2026-10-14; URMA is not used).

Moves go through hz_gust_qc_ab_quarantine (re-checks each key server-side), reason 'qc v5.2 rules A/B (nightly):
...', so hz_gust_suspect_guard blocks later re-inserts. Same window and switches as station_gust_qc.py.
"""
import datetime as dt

import feedguard as fg
import station_gust_qc as q

UTC = dt.timezone.utc
QC_KEY = "gust-qc-ab"
MAX_REMOVALS_PER_DAY = int(fg.env_float("GUST_QC_AB_MAX_PER_DAY", 12))   # archive max: 8 (2024-04-01)
MAX_CANDIDATES = int(fg.env_float("GUST_QC_AB_MAX_CANDIDATES", 80))
BATCH = 25


def own_reports(r, fetch_metars=q.metars):
    """(match, thunder, day_max_mph) from the station's own METAR/SPECI on its local day."""
    g = float(r["g"])
    tz = q.local_zone(r)
    L0 = dt.datetime.combine(dt.date.fromisoformat(r["date"]), dt.time(0), tz).astimezone(UTC)
    L1 = L0 + dt.timedelta(days=1)
    obs = fetch_metars(r["stid"], L0 - dt.timedelta(hours=2), L1 + dt.timedelta(hours=2))
    cands = []
    for o in obs:
        if o["gust"] is not None and L0 <= o["t"] < L1:
            cands.append((o["t"], o["gust"] * q.KT))
        if o["pk"] is not None:
            te = o["pkt"] or o["t"]
            if L0 <= te < L1:
                cands.append((te, o["pk"] * q.KT))
    hits = sorted([c for c in cands if abs(c[1] - g) <= 2], key=lambda c: abs(c[1] - g))
    peak = hits[0][0] if hits else (dt.datetime.fromisoformat(r["peak_time_utc"]) if r.get("peak_time_utc") else None)
    thunder = False
    if peak:
        near = [o for o in obs if abs((o["t"] - peak).total_seconds()) <= 3600]
        thunder = any(("TS" in o["wx"]) or (" LTG" in o["metar"] and "LTG DSNT" not in o["metar"]) for o in near)
    return bool(hits), thunder, round(max([c[1] for c in cands], default=0), 1)


def _f(v):
    return None if v is None else float(v)


def decide(r, match, thunder, hurdat_end):
    """('REMOVE' | 'KEEP' | 'HOLD', why) for one hz_gust_qc_ab_candidates row."""
    g = float(r["g"])
    max50, max75, max100, meas25, hrrr = (_f(r.get(k)) for k in ("max50", "max75", "max100", "meas25", "hrrr"))
    hu = r.get("hurricane")
    if r["rule"] in ("A", "A2"):
        if not match:
            v, why = ("REMOVE", "not in its own reports") if r["rule"] == "A" else ("KEEP", "A2: not in its own reports (not obvious)")
        else:
            backup = []
            if max50 is not None and max50 >= 0.6 * g:
                backup.append(f"normal station within 50 mi at {r.get('top50')}")
            if r.get("wind15"):
                backup.append("NWS wind report within 15 mi")
            if thunder and (r.get("any50") or (max100 or 0) >= 45):
                backup.append("own thunderstorm + nearby storm evidence")
            if hu:
                backup.append(f"hurricane {hu}")
            if backup:
                v, why = "KEEP", "; ".join(backup)
            elif r["rule"] == "A2" and (max50 is None or g < 2 * max50):
                v, why = "KEEP", "A2: not an obvious error"
            else:
                v, why = "REMOVE", f"no backup; best normal station within 50 mi {r.get('top50') or 'none'}"
    else:
        if hu:
            return "KEEP", f"hurricane {hu} within 150 mi"
        if g > 130:
            v, why = "REMOVE", "over 130 mph with no hurricane"
        elif not match:
            v, why = "REMOVE", "not in its own reports"
        else:
            backup = []
            if max75 is not None and max75 >= 0.8 * g:
                backup.append(f"station within 75 mi at {r.get('top75')}")
            if (r.get("n75_100") or 0) >= 2:
                backup.append(f"{r['n75_100']} stations within 100 mi at 75+")
            if meas25 is not None and meas25 >= 0.75 * g:
                backup.append(f"measured NWS report {meas25:g} mph within 25 mi")
            v, why = ("KEEP", "; ".join(backup)) if backup else ("REMOVE", f"no backup; best station within 75 mi {r.get('top75') or 'none'}")
        if v == "REMOVE":
            if hurdat_end and r["date"] > hurdat_end and (r.get("n75_100") or 0) >= 2:
                return "HOLD", f"hurricane tracks not published for this date and {r['n75_100']} stations within 100 mi read 75+ (would be: {why})"
            if match and r.get("storm_day") and not r.get("glitch_record"):
                return "HOLD", f"storm day at a station with no glitch record (would be: {why})"
    if v == "REMOVE" and match and hrrr is not None and hrrr >= 58 and hrrr >= 0.6 * g:
        return "KEEP", f"HRRR grid {hrrr:g} mph at the station (>= 58 and >= 60%)"
    return v, why


def run_ab(run, dates, explicit, own_fn=own_reports):
    s = {"dates": [], "candidates": 0, "verdicts": {}, "quarantined": [], "held": [], "fetch_failed": []}
    run.meta["gust_qc_ab"] = s
    rows, hurdat_end = [], None
    for d0, d1 in q._ranges(q.qc_dates(dates, explicit)):
        r = run.rpc_read("hz_gust_qc_ab_candidates", {"p_secret": run.secret, "p_d0": d0.isoformat(), "p_d1": d1.isoformat()})
        res = r.json() if hasattr(r, "json") else None
        if not isinstance(res, dict) or "rows" not in res:
            raise fg.FeedError(f"hz_gust_qc_ab_candidates {d0}..{d1}: unexpected reply")
        hurdat_end = res.get("hurdat_end") or hurdat_end
        s["dates"] += [(d0 + dt.timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]
        rows += res["rows"]
    s["candidates"] = len(rows)
    if len(rows) > MAX_CANDIDATES:
        run.warn(QC_KEY, f"{len(rows)} candidates; only the first {MAX_CANDIDATES} are checked this run")
        rows = rows[:MAX_CANDIDATES]
    removals = {}
    for x in rows:
        tag = f"{x['stid']} {x['date']} {x['g']} mph rule {x['rule']}"
        try:
            match, thunder, day_max = own_fn(x)
        except q.FetchFailed as e:
            s["fetch_failed"].append(tag)
            run.note(QC_KEY, f"{tag}: station reports unavailable ({e}); checked by a later run")
            continue
        v, why = decide(x, match, thunder, hurdat_end)
        s["verdicts"][v] = s["verdicts"].get(v, 0) + 1
        print(f"  [gust-qc-ab] {tag} -> {v} ({why})", flush=True)
        if v == "HOLD":
            s["held"].append(f"{tag}: {why}")
            run.warn(QC_KEY, f"HELD for review (not moved): {tag}: {why}")
        elif v == "REMOVE":
            removals.setdefault(x["date"], []).append({"stid": x["stid"], "date": x["date"], "rule": x["rule"],
                                                       "why": why[:300], "g": x["g"]})
    for day, rm in sorted(removals.items()):
        if len(rm) > MAX_REMOVALS_PER_DAY:
            s["held"] += [f"{f['stid']} {day}" for f in rm]
            run.warn(QC_KEY, f"{day}: {len(rm)} rule A/B removals (> {MAX_REMOVALS_PER_DAY}); nothing moved for this "
                             f"date - review by hand: {', '.join(f['stid'] for f in rm)}")
            continue
        for i in range(0, len(rm), BATCH):
            b = rm[i:i + BATCH]
            run.write(QC_KEY, f"gust-qc-ab {day}", [("hz_gust_qc_ab_quarantine", {
                "p_secret": run.secret, "p_rows": [{k: f[k] for k in ("stid", "date", "rule", "why")} for f in b]})])
            for f in b:
                s["quarantined"].append(f"{f['stid']} {day} {f['g']} mph rule {f['rule']}: {f['why']}")
                run.note(QC_KEY, f"{'would quarantine' if run.dry_run else 'quarantined'} {f['stid']} {day} "
                                 f"{f['g']} mph (rule {f['rule']}: {f['why']})")
        run.written(QC_KEY, f"gust-qc-ab {day}")
    print(f"[gust-qc-ab] dates {s['dates']} candidates {s['candidates']} verdicts {s['verdicts']} "
          f"quarantined {len(s['quarantined'])} held {len(s['held'])}", flush=True)
    return s
