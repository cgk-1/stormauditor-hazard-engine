# stormauditor-hazard-engine

## Operations (Archive Phase 5 Stage 1, 2026-10-07)
- `DATE=YYYY-MM-DD` or `DATE=START..END` (workflow input `ingest_date`; also `YYYYMMDD`, `a:b`, comma lists). Blank = yesterday.
- `DRY_RUN=1` (input `dry_run`), the `FEED_RESULT {json}` last line, quarantine records in `feed-out/` (artifact), and a non-zero exit on any failure: see `feedguard.py` (shared verbatim with the hail and wind feeds).
- HRRR: every hour needs both f01 fields (WIND 0-1 h max and GUST). Only the allow-listed hours in `HRRR_F01_FALLBACK_OK` may fall back to the f00 GUST analysis, and that is logged. Scheduled runs defer the day if a recent hour is not published yet.
- Obs: per-network failures fail the job while the complete networks are written. DC has no IEM ASOS network, which is expected. LSR rows broken by an unquoted comma are repaired only when the repair is unambiguous; otherwise they are quarantined.
- Deps are pinned in `requirements.txt`. Census 500k 2019 state boundaries are vendored and md5-checked in `data/`.
