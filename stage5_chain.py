#!/usr/bin/env python3
"""Stage 5 self-chain: the last job of a LIVE stage5-backfill run starts the next live run.

Why (owner 2026-10-08 "self-chain it"): GitHub's "4,34 * * * *" schedule for this workflow never
fired once (every run since 00:39Z 10-08 was a workflow_dispatch), so each run that ended at a
forbidden window or its time budget left the backfill idle until someone dispatched it again.

Rules (the backfill's own gates are unchanged; this only decides WHEN to dispatch the next run):
  - never chains a dry run or a cancelled run (the workflow's `if:`), so cancelling a run stops it;
  - stops if the workflow is not 'active' (self-disabled: complete / 20 GB guard / 3rd failure of a
    day, or disabled by hand) - checked before and after any wait;
  - stops if this run's plan job did not succeed, or if this run and the 2 runs before it all failed;
  - inside a forbidden window, or < 15 min before one (the planner would refuse), it waits until
    the window ends + 1 min; a run that did no work for another reason (feed run / DB load) waits
    10 min - so it can never spin;
  - skips the dispatch if another stage5-backfill run is already queued or running.
Env: GH_TOKEN, GITHUB_REPOSITORY, GITHUB_RUN_ID, PLAN_RESULT, SHARD_RESULT, ROLL_RESULT, PLAN_GO.
CHAIN_DRY=1 prints the decision without waiting or dispatching.
"""
import datetime as dt
import json
import os
import sys
import time
import urllib.error
import urllib.request

from stage5_backfill import FORBID, minutes_to_forbidden, now

WF = "stage5-backfill.yml"
LEAD_MIN = 15          # the planner needs >= 10 min of budget (mtf - 5); stay clear of that edge
IDLE_WAIT_MIN = 10     # no work for a non-window reason (feed run / DB load)
MAX_WAIT_MIN = 145     # the job's timeout is 160


def gh(method, path, body=None):
    req = urllib.request.Request(f"https://api.github.com/{path}", method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": f"Bearer {os.environ['GH_TOKEN']}",
                                          "Accept": "application/vnd.github+json",
                                          "X-GitHub-Api-Version": "2022-11-28"})
    with urllib.request.urlopen(req, timeout=30) as r:
        raw = r.read()
        return r.status, (json.loads(raw) if raw else None)


def say(msg):
    print(msg, flush=True)
    sp = os.environ.get("GITHUB_STEP_SUMMARY")
    if sp:
        with open(sp, "a") as fh:
            fh.write(f"- {msg}\n")


def wait_minutes(t, did_work):
    """Minutes to wait before dispatching (0 = now)."""
    m = t.hour * 60 + t.minute + t.second / 60
    mtf = minutes_to_forbidden(t)
    if mtf < LEAD_MIN:
        for a, b in FORBID:
            if a <= m < b:
                return b - m + 1
        start = (m + mtf) % (24 * 60)
        for a, b in FORBID:
            if abs(a - start) < 1e-6:
                return mtf + (b - a) + 1
    return 0 if did_work else IDLE_WAIT_MIN


def active(repo):
    return gh("GET", f"repos/{repo}/actions/workflows/{WF}")[1].get("state") == "active"


def main():
    repo, run_id = os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_RUN_ID"]
    res = {k: (os.environ.get(f"{k}_RESULT") or "").lower() for k in ("PLAN", "SHARD", "ROLL")}
    did_work = (os.environ.get("PLAN_GO") or "").lower() == "true"
    dry = (os.environ.get("CHAIN_DRY") or "") == "1"
    say(f"chain: job results {res}, plan go={did_work}")
    if not active(repo):
        say("chain: STOP - workflow is not active (self-disabled or disabled by hand)")
        return 0
    if res["PLAN"] != "success":
        say("::warning::chain: STOP - the plan job did not succeed; dispatch the next run by hand after a look")
        return 0
    if "failure" in res.values():
        runs = gh("GET", f"repos/{repo}/actions/workflows/{WF}/runs?per_page=10&status=completed")[1]
        prev = [r["conclusion"] for r in runs.get("workflow_runs", []) if str(r["id"]) != run_id][:2]
        if len(prev) == 2 and all(c == "failure" for c in prev):
            say("::warning::chain: STOP - this run and the 2 before it failed; needs a look")
            return 0
    w = min(wait_minutes(now(), did_work), MAX_WAIT_MIN)
    say(f"chain: wait {w:.0f} min, then dispatch the next live run")
    if dry:
        return 0
    if w > 0:
        time.sleep(w * 60)
        if not active(repo):
            say("chain: STOP - workflow was disabled while waiting")
            return 0
    for st in ("queued", "in_progress", "waiting", "pending"):
        runs = gh("GET", f"repos/{repo}/actions/workflows/{WF}/runs?per_page=10&status={st}")[1]
        other = [r["id"] for r in runs.get("workflow_runs", []) if str(r["id"]) != run_id]
        if other:
            say(f"chain: another run is already {st} ({other[0]}) - not dispatching")
            return 0
    code, _ = gh("POST", f"repos/{repo}/actions/workflows/{WF}/dispatches",
                 {"ref": "main", "inputs": {"dry_run": "0"}})
    say(f"chain: dispatched the next live run at {now():%H:%M}Z (HTTP {code})")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except urllib.error.HTTPError as e:
        say(f"::error::chain: GitHub API HTTP {e.code}: {e.read()[:200]!r}")
        sys.exit(1)
