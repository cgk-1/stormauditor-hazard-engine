#!/usr/bin/env python3
"""
StormAuditor HAZARD ENGINE - nightly station-gust QC (v5.2; owner 2026-10-13 "yes start on #1
the nightly check").

Runs at the end of the obs ingest (hazard_obs_ingest.py main) so a new sensor-fault gust never
reaches reports. It is an add-on: any failure here is a WARNING, never an ingest failure.

  1. hz_gust_qc_candidates (read RPC): the station daily gusts 50-<100 mph that stand far above
     every station within 40 mi (> max(2 x their max, max + 30); with no neighbour: > max(2.5 x
     HRRR, HRRR + 30)), unless an independent LSR within 25 mi or the HRRR background (>= 0.6 x the
     gust) supports them. Offshore platforms and summit/ridge stations (hz_gust_qc_exempt) are
     never candidates. Same SQL as the validated archive run (docs/qc/station-gust-qc-proto.sql).
  2. classify() (this file): the verdict from the station's OWN reports - IEM METAR/SPECI around
     the matching gust and, when complete, the ASOS 1-minute record - plus whether NWS echoed the
     value as an LSR. Identical rules to docs/qc/station-gust-classify-v51.py + the v5.2 HRRR
     guard (validated 2026-10-13: 67/68 decided blind cases right, 0/296 measured Storm Data gusts
     removed; docs/station-gust-qc-prototype-2026-10-13-steps.md).
  3. Only FAULT verdicts are acted on: hz_gust_qc_quarantine moves the reading (v3 daily, v4 daily,
     peak row) to hz_gust_quarantine with reason 'qc v5.2 sensor fault (nightly): <why>'. The RPC
     re-checks server-side that each key is a current candidate; hz_gust_suspect_guard then blocks
     every later re-insert of that key. REAL / UNCLEAR / UNSUPPORTED readings are kept.

Window: scheduled runs check yesterday and the two days before (a date waits until its HRRR
station backgrounds are in - the hrrr job of the same workflow writes them - so the 09:20 run
usually checks yesterday on the next dispatch). Explicit-date runs (repairs, backfills) skip the
check unless GUST_QC=1. GUST_QC=0 turns it off. DRY_RUN=1: verdicts are computed and logged, the
quarantine call is only recorded.
Safety stops: more than MAX_FAULTS_PER_DAY faults on one date (archive max: 2) -> nothing is moved
for that date and the run warns; at most MAX_CANDIDATES readings are classified per run.
"""
import csv, io, os, re, datetime as dt
from zoneinfo import ZoneInfo

import feedguard as fg

UTC = dt.timezone.utc
IEM = "https://mesonet.agron.iastate.edu"
UA = {"User-Agent": "StormAuditor-HazardEngine/1.0 (station gust QC)"}
KT = 1.15078
STORM_WX = ('TS', 'SQ', 'GR', 'GS', 'FC', 'SH')
STORM_RMK = (' VIRGA', ' CB', ' TCU', ' VCSH', 'WSHFT', 'PRESRR', 'FROPA', 'TSB', 'RAB', 'SHRAB')
REAL_SIGNS = ('storm_wx', 'storm_rmk', 'ltg_near', 'temp_drop', 'heat_burst', 'adj_strong', 'own_strong',
              'echo', 'onemin_strong')
QC_KEY = "gust-qc"
LOOKBACK_DAYS = 2
HRRR_MIN_ROWS = 2000            # a complete day has one HRRR background per station (2,715)
MAX_FAULTS_PER_DAY = int(fg.env_float("GUST_QC_MAX_FAULTS_PER_DAY", 5))
MAX_CANDIDATES = int(fg.env_float("GUST_QC_MAX_CANDIDATES", 60))
QUARANTINE_BATCH = 10           # hz_gust_qc_quarantine accepts at most 10 rows per call


class FetchFailed(Exception):
    pass


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _get_text(url):
    try:
        raw = fg.http_get(url, headers=UA, timeout=90, retries=3, missing_ok=True, what="IEM qc")
    except fg.FeedError as e:
        raise FetchFailed(str(e)) from None
    return None if raw is None else raw.decode(errors="replace")


def _z(t):
    return t.strftime('%Y-%m-%dT%H:%MZ')


def metars(stid, a, b):
    """The station's METAR + SPECI reports in [a, b] (IEM asos.py)."""
    url = (f"{IEM}/cgi-bin/request/asos.py?station={stid}&data=sknt&data=gust&data=peak_wind_gust"
           f"&data=peak_wind_time&data=tmpf&data=dwpf&data=wxcodes&data=metar&sts={_z(a)}&ets={_z(b)}"
           f"&tz=UTC&format=onlycomma&missing=M&report_type=3&report_type=4")
    text = _get_text(url)
    if text is None:
        raise FetchFailed(f"asos.py {stid}: not found")
    out = []
    for r in csv.DictReader(io.StringIO(text)):
        if 'valid' not in r or not r['valid']:
            raise FetchFailed(f"asos.py {stid}: unexpected reply {text[:80]!r}")
        o = {'t': dt.datetime.strptime(r['valid'], '%Y-%m-%d %H:%M').replace(tzinfo=UTC), 'sk': num(r.get('sknt')),
             'gust': num(r.get('gust')), 'pk': num(r.get('peak_wind_gust')), 'tmpf': num(r.get('tmpf')),
             'dwpf': num(r.get('dwpf')), 'wx': r.get('wxcodes') or '', 'metar': r.get('metar') or ''}
        pt = r.get('peak_wind_time') or ''
        o['pkt'] = (dt.datetime.strptime(pt, '%Y-%m-%d %H:%M').replace(tzinfo=UTC)
                    if re.match(r'\d{4}-\d\d-\d\d \d\d:\d\d', pt) else None)
        out.append(o)
    return out


def onemin(stid, te):
    """ASOS 1-minute wind +-20 min around te; None unless the record is ~complete (>= 35 rows).
    Missing 1-minute data is normal for recent days and never counts as calm."""
    a, b = te - dt.timedelta(minutes=20), te + dt.timedelta(minutes=20)
    url = (f"{IEM}/cgi-bin/request/asos1min.py?station={stid}&vars=sknt&vars=gust_sknt&sts={_z(a)}&ets={_z(b)}"
           f"&sample=1min&what=download&delim=comma&gis=no")
    try:
        text = _get_text(url)
    except FetchFailed:
        return None
    if not text:
        return None
    rows = [r for r in csv.DictReader(io.StringIO(text))
            if num(r.get('sknt')) is not None and num(r.get('gust_sknt')) is not None and r.get('valid(UTC)')]
    if len(rows) < 35:
        return None
    for r in rows:
        r['t'] = dt.datetime.strptime(r['valid(UTC)'], '%Y-%m-%d %H:%M').replace(tzinfo=UTC)
    return rows


def local_zone(r):
    if r.get('tz'):
        return ZoneInfo(r['tz'])
    lon = float(r['lon'])
    return ZoneInfo('America/Denver' if lon < -104 else 'America/Chicago' if lon < -94 else 'America/New_York')


def classify(r, fetch_metars=metars, fetch_onemin=onemin):
    """v5.2 verdict for one candidate row (hz_gust_qc_candidates). Returns (verdict, features);
    verdict in FAULT / REAL / UNCLEAR / UNSUPPORTED. Raises FetchFailed when the METARs are unavailable."""
    stid, g, date = r['stid'], float(r['g']), dt.date.fromisoformat(r['date'])
    tz = local_zone(r)
    L0 = dt.datetime.combine(date, dt.time(0), tz).astimezone(UTC)
    L1 = L0 + dt.timedelta(days=1)
    obs = fetch_metars(stid, L0 - dt.timedelta(hours=2), L1 + dt.timedelta(hours=2))
    peak_t = dt.datetime.fromisoformat(r['peak_time_utc'].replace('Z', '+00:00')) if r.get('peak_time_utc') else None
    cands = []   # (event time, mph, obs, kind): a body gust at its report time, a PK WND at its own time
    for o in obs:
        if o['gust'] is not None and L0 <= o['t'] < L1:
            cands.append((o['t'], o['gust'] * KT, o, 'body'))
        if o['pk'] is not None:
            te = o['pkt'] or o['t']
            if L0 <= te < L1:
                cands.append((te, o['pk'] * KT, o, 'pk'))
    day_max = max([c[1] for c in cands], default=0)
    hits = [c for c in cands if abs(c[1] - g) <= 3.5]
    if peak_t:
        hits = sorted(hits, key=lambda c: abs((c[0] - peak_t).total_seconds()))
    if not hits:
        return ('UNSUPPORTED' if g - day_max >= 15 else 'UNCLEAR'), {'why': 'nomatch', 'day_max_metar_mph': round(day_max, 1)}
    te, _mph, ho, kind = hits[0]
    f = {'kind': kind, 'te': te.isoformat(), 'metar': ho['metar'][:110]}

    def near(m):
        return [o for o in obs if abs((o['t'] - te).total_seconds()) <= m * 60]
    n60 = near(60)
    others = [o for o in obs if o is not ho]
    before = [o for o in others if o['t'] <= te and (te - o['t']).total_seconds() <= 20 * 60]
    after = [o for o in others if o['t'] > te and (o['t'] - te).total_seconds() <= 20 * 60]
    ao1 = ('AO1' in ho['metar'] or 'A01' in ho['metar'])
    west = float(r['lon']) < -104
    sparse = int(r.get('n40') or 0) == 0
    sk = ho['sk'] if kind == 'body' else None
    one = fetch_onemin(stid, te)
    sig = {}
    # phantoms (FAULT regardless of context)
    gtxt = re.search(r'\b\d{3}\d{2,3}G\d{2,3}KT\b|\bVRB\d{2,3}G\d{2,3}KT\b', ho['metar'])
    sig['cor_no_gust'] = ' COR ' in f' {ho["metar"]} ' and kind == 'body' and not gtxt and 'PK WND' not in ho['metar']
    # "calm with a gust" is impossible only on ASOS (AO2); AWOS/AO1 rules differ
    sig['calm_gust'] = kind == 'body' and 'AO2' in ho['metar'] and bool(re.search(r'\b00000G\d+KT\b', ho['metar']))
    if one:
        g1 = max((num(x['gust_sknt']) or 0) for x in one) * KT
        s1 = [num(x['sknt']) for x in one]
        sig['onemin_max_mph'] = round(g1, 1)
        sig['onemin_unsupported'] = g1 > 0 and g1 < g - 15
        sig['onemin_strong'] = sum(1 for v in s1 if v is not None and v >= 20) >= 3
        hi = [x for x in one if (num(x['gust_sknt']) or 0) * KT >= g - 5]
        sig['onemin_spike'] = len(hi) <= 1 and not sig['onemin_strong'] and max((v or 0) for v in s1) <= 8
    # REAL signs
    sig['storm_wx'] = any(any(c in o['wx'] for c in STORM_WX) for o in n60)
    sig['storm_rmk'] = any(any(k in f' {o["metar"]}' for k in STORM_RMK) for o in n60)
    ltg = [o for o in n60 if ' LTG' in o['metar']]
    sig['ltg_near'] = any('LTG DSNT' not in o['metar'] for o in ltg)
    sig['ltg_dsnt_west'] = west and any('LTG DSNT' in o['metar'] for o in ltg)
    tb = [o for o in obs if 0 < (te - o['t']).total_seconds() <= 3600 and o['tmpf'] is not None]
    ta = [o for o in obs if 0 <= (o['t'] - te).total_seconds() <= 3600 and o['tmpf'] is not None]
    if tb and ta:
        dT = (min(o['tmpf'] for o in ta) - tb[-1]['tmpf']) / 1.8
        rise = (max(o['tmpf'] for o in ta) - tb[-1]['tmpf']) / 1.8
        dTd = None
        if tb[-1]['dwpf'] is not None and any(o['dwpf'] is not None for o in ta):
            dTd = (min(o['dwpf'] for o in ta if o['dwpf'] is not None) - tb[-1]['dwpf']) / 1.8
        sig['temp_drop'] = dT <= -3
        sig['heat_burst'] = rise >= 3 and dTd is not None and dTd <= -3
    sig['adj_strong'] = any((o['sk'] or 0) >= 20 for o in near(30) if o is not ho)
    sig['own_strong'] = sk is not None and sk >= 20 and any((o['sk'] or 0) >= 12 for o in near(30) if o is not ho)
    sig['echo'] = int(r.get('echo') or 0) > 0
    # FAULT signs (positive evidence only - missing data never counts)
    b1, a1 = before[-1:], after[:1]
    sig['calm_both'] = (len(b1) == 1 and len(a1) == 1
                        and all(o['sk'] is not None and o['sk'] <= 6 and not o['gust'] for o in b1 + a1))
    sig['ratio'] = kind == 'body' and sk is not None and sk <= 10 and (ho['gust'] or 0) >= 3 * max(sk, 1)
    f.update(sig)
    f.update({'ao1': ao1, 'sparse': sparse, 'west': west})
    if sig['cor_no_gust'] or sig['calm_gust']:
        f['why'] = 'fault:phantom'
        return 'FAULT', f
    real = [k for k in REAL_SIGNS if sig.get(k)]
    if real:
        f['why'] = 'real:' + ','.join(real)
        return 'REAL', f
    if sig.get('onemin_unsupported'):
        f['why'] = 'fault:onemin_unsupported'
        return 'FAULT', f
    if sig['calm_both'] or sig.get('onemin_spike'):
        f['why'] = 'fault:calm_both' if sig['calm_both'] else 'fault:onemin_spike'
        return 'FAULT', f
    if sig['ratio'] and not sparse and not ao1 and not sig['ltg_dsnt_west']:
        hrrr = r.get('hrrr')
        if hrrr is not None and float(hrrr) >= 0.4 * g:
            # v5.2: a regional wind event (HRRR background >= 0.4 x the gust) is never a ratio-only fault
            f['why'] = 'unclear:ratio_regional_wind'
            return 'UNCLEAR', f
        f['why'] = 'fault:ratio'
        return 'FAULT', f
    f['why'] = 'unclear' + (':ltg_dsnt_west' if sig['ltg_dsnt_west'] else '')
    return 'UNCLEAR', f


def enabled(explicit):
    v = (os.environ.get("GUST_QC") or "").strip().lower()
    if v in ("0", "false", "no", "off"):
        return False
    if v in ("1", "true", "yes", "on"):
        return True
    return not explicit


def qc_dates(dates, explicit):
    if explicit:
        return sorted(set(dates))
    last = max(dates)
    return [last - dt.timedelta(days=i) for i in range(LOOKBACK_DAYS, -1, -1)]


def _ranges(days, span=7):
    days = sorted(days)
    out = []
    for d in days:
        if out and (d - out[-1][0]).days < span and d == out[-1][1] + dt.timedelta(days=1):
            out[-1][1] = d
        else:
            out.append([d, d])
    return out


def run_qc(run, dates, explicit, classify_fn=classify):
    """Check the given dates; returns the summary dict (also stored in run.meta['gust_qc'])."""
    summary = {"dates": [], "waiting_hrrr": [], "candidates": 0, "verdicts": {}, "quarantined": [],
               "held": [], "fetch_failed": []}
    run.meta["gust_qc"] = summary
    rows = []
    for d0, d1 in _ranges(qc_dates(dates, explicit)):
        r = run.rpc_read("hz_gust_qc_candidates", {"p_secret": run.secret, "p_d0": d0.isoformat(),
                                                   "p_d1": d1.isoformat()})
        res = r.json() if hasattr(r, "json") else None
        if not isinstance(res, dict) or "rows" not in res or "hrrr" not in res:
            raise fg.FeedError(f"hz_gust_qc_candidates {d0}..{d1}: unexpected reply")
        ready = {k for k, n in (res.get("hrrr") or {}).items() if int(n or 0) >= HRRR_MIN_ROWS}
        for i in range((d1 - d0).days + 1):
            day = (d0 + dt.timedelta(days=i)).isoformat()
            (summary["dates"] if day in ready else summary["waiting_hrrr"]).append(day)
        rows += [x for x in res["rows"] if x["date"] in ready]
    if summary["waiting_hrrr"]:
        run.note(QC_KEY, f"HRRR station backgrounds not in yet for {', '.join(summary['waiting_hrrr'])}: "
                         f"checked by a later run")
    summary["candidates"] = len(rows)
    if len(rows) > MAX_CANDIDATES:
        run.warn(QC_KEY, f"{len(rows)} candidates; only the first {MAX_CANDIDATES} are checked this run")
        rows = rows[:MAX_CANDIDATES]
    faults = {}
    for x in rows:
        tag = f"{x['stid']} {x['date']} {x['g']} mph"
        try:
            v, feats = classify_fn(x)
        except FetchFailed as e:
            summary["fetch_failed"].append(tag)
            run.note(QC_KEY, f"{tag}: station reports unavailable ({e}); checked by a later run")
            continue
        summary["verdicts"][v] = summary["verdicts"].get(v, 0) + 1
        print(f"  [gust-qc] {tag} -> {v} ({feats.get('why')})", flush=True)
        if v == "FAULT":
            faults.setdefault(x["date"], []).append({"stid": x["stid"], "date": x["date"], "why": feats["why"],
                                                      "g": x["g"], "metar": feats.get("metar", "")})
    for day, fl in sorted(faults.items()):
        if len(fl) > MAX_FAULTS_PER_DAY:
            summary["held"] += [f"{f['stid']} {day}" for f in fl]
            run.warn(QC_KEY, f"{day}: {len(fl)} sensor-fault verdicts (> {MAX_FAULTS_PER_DAY}); nothing moved "
                             f"for this date - review by hand: {', '.join(f['stid'] for f in fl)}")
            continue
        for i in range(0, len(fl), QUARANTINE_BATCH):
            batch = fl[i:i + QUARANTINE_BATCH]
            payload = {"p_secret": run.secret,
                       "p_rows": [{"stid": f["stid"], "date": f["date"], "why": f["why"]} for f in batch]}
            run.write(QC_KEY, f"gust-qc {day}", [("hz_gust_qc_quarantine", payload)])
            for f in batch:
                summary["quarantined"].append(f"{f['stid']} {day} {f['g']} mph {f['why']}")
                run.note(QC_KEY, f"{'would quarantine' if run.dry_run else 'quarantined'} {f['stid']} {day} "
                                 f"{f['g']} mph ({f['why']}): {f['metar'][:90]}")
        run.written(QC_KEY, f"gust-qc {day}")
    print(f"[gust-qc] dates {summary['dates']} waiting {summary['waiting_hrrr']} candidates {summary['candidates']} "
          f"verdicts {summary['verdicts']} quarantined {len(summary['quarantined'])} held {len(summary['held'])}",
          flush=True)
    return summary
