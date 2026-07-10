# StormAuditor HAZARD ENGINE — architecture & setup

## What this is

A commercial-suite-class system: national multi-source processing happens
**once per day** in free GitHub Actions pipelines; the per-address report is a
**sub-second Supabase RPC** that fuses seven layers. No downloads, no waiting,
no per-address GB — the exact pattern Benchmark-class products use
("radar, numerical weather models, observations, proprietary algorithms" —
processed ahead of time, served instantly).

## Feature parity vs commercial wind/hail reports

| Commercial report feature | Hazard Engine |
|---|---|
| Max gust at property + distance rings | ✅ at-property + 1/3/10 mi, from TWO independent grids |
| Multiple data sources fused | ✅ 7 layers: URMA/RTMA analysis, HRRR model, ASOS measured gusts, NWS LSRs, finalized NCEI Storm Events, NHC best track, MRMS MESH |
| Duration at/above thresholds | ✅ hours ≥ 40 and ≥ 58 mph at the property cell (hourly analysis resolution, stated) |
| Hail size at property + rings | ✅ MESH property/1/3/10 mi (existing feed) + report/event corroboration |
| Prior & subsequent events | ✅ full 4-year event list in one call |
| Tropical vs non-tropical flag | ✅ HURDAT2 proximity flag per day |
| Measured vs estimated distinction | ✅ LSR qualifier + NCEI MG/EG kept per report |
| Confidence statement | ✅ fused best-estimate, range, A/B/C grade, sources_active count |
| Instant delivery | ✅ one RPC, <1 s |

The proprietary asset is the **estimator**: `property_hazard_report()`
returns a single per-address MAX ESTIMATED WIND (mph) and MAX ESTIMATED HAIL
(inches) in `summary`, plus the same estimate for every event day — computed
by two documented, deterministic algorithms whose every input ships with the
answer:

**SAWE-1 (wind)**: analysis-of-record background at the property cell → +50%
of any upward HRRR disagreement (radar-assimilating HRRR resolves convective
swaths the analysis smooths; downward disagreement is not credited because
the analysis assimilates observations) → distance-weighted station residual
correction (measured minus analysis at stations ≤30 mi, weights 1/(1+d/10)²,
clamped ±15 mph) → clamped between the weakest at-property source and the
strongest 1-mile/measured evidence. Validated in Postgres with the real
Milton values: raw analysis said 55 mph at the Orlando address; SAWE-1
produced **75 mph, grade A, range 55–86**, driven by HRRR (65) and MCO's
measured 86.3 at 8.9 mi — the number an adjuster-grade report would defend.

**SAHE-1 (hail)**: MESH at-property 1 km cell → adjacent-cell inference
(0.85× the 1-mile max) when no at-property cell → ground-report
reconciliation (reports ≤3 mi floor the estimate at min(report size, 3-mile
MESH max)) → verdict category (confirmed_nearby / likely / possible /
in_area_only). Validated both ways: an out-of-range report does not inflate
the estimate; an in-range 1.25″ report correctly floors it at the
radar-supported 0.55″ with method `report_reconciliation`.

## The pipelines (repo: stormauditor-hazard-engine)

| File | Runs | Does |
|---|---|---|
| `hazard_wind_ingest.py` | daily 09:40Z | 48 byte-ranged GUST slices (24 URMA/RTMA + 24 HRRR) → per-cell daily max + duration hours → `hz_wind_points` (cells ≥ 40 mph, tagged ANL/HRRR) |
| `hazard_obs_ingest.py` | daily + weekly | ASOS daily peak gusts (all CONUS networks), national LSRs; weekly: station metadata, NCEI Storm Events, HURDAT2 |
| `hazard_backfill_walker.py` | every 2 h until done | walks both wind layers back 4 years, budget-safe, cursor-saved per day |
| `supabase/hazard_schema.sql` | once | tables, locked ingest RPCs, and `property_hazard_report()` — the fusion RPC |

Your existing repos stay as-is: the MESH hail feed keeps feeding the hail
layer (extend its walker/purge to 1461 days), and the old wind feed can be
retired once `hz_wind_points` backfills past it (the hazard layer is a strict
superset: dual-source + durations per cell).

## Setup

**1. GitHub:** new private repo with the four files + `.github/workflows/`
(daily.yml, weekly.yml, backfill.yml). Add secrets `SUPABASE_URL`,
`SUPABASE_ANON_KEY`, `INGEST_SECRET` (same values as your other feeds).

**2. Lovable — schema.** Paste:

> Create a migration from the SQL I'm pasting. Keep every table name,
> function name, and signature exactly as written — external GitHub pipelines
> call these RPCs. One adaptation IS required: inside
> `property_hazard_report`, the hail block reads from
> `hail_points(date,lon,lat,v)` — replace that one reference with our actual
> MESH hail points table and columns (the table our existing hail feed's
> ingest_points RPC writes to). The RLS-without-policies on the hz_ tables
> and sa_config is intentional; do not add policies.
>
> [paste supabase/hazard_schema.sql]

Then (if not already done): store the ingest secret —
`insert into sa_config(key,value) values ('ingest_secret','YOUR_SECRET') on
conflict (key) do update set value=excluded.value;`

**3. Prime the layers** (GitHub → Actions, run manually once):
- `hazard-weekly` → loads stations, NCEI (set ncei_years to
  `2022,2023,2024,2025,2026`), HURDAT2. ~10 min.
- `hazard-daily` with ingest_date `20260601:20260709` for obs, and let the
  wind job do yesterday. Backfill of observations: run `hazard-daily` obs job
  with year ranges (`20250101:20251231`, etc.) — station dailies and LSRs
  support ranges in one request per network/year.
- `hazard-backfill` → starts walking wind history backward immediately.
  ~2–4 min per day per source ⇒ the last 12 months land within the first
  couple days of scheduled runs; full 4 years in ~2–3 weeks of free runners,
  reports deepening the whole time.

**4. Lovable — frontend.** Paste:

> Build the Property Hazard Report page. On submit, geocode the address with
> the US Census geocoder
> (https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=...&benchmark=Public_AR_Current&format=json,
> lon=coordinates.x, lat=coordinates.y), then call the RPC
> `property_hazard_report` with p_lat/p_lon and render instantly.
> Wind table (from wind_days, newest first): Date; Best estimate
> (`fused.best_estimate_mph`); Range (`fused.range_mph` as "lo–hi");
> Analysis at/1mi/3mi/10mi; HRRR at/1mi/10mi; Hrs ≥40 / ≥58; Nearest measured
> gust (mph @ dist); Grade; a 🌀 icon when `tropical`. Expandable row panel:
> `storm_reports` (show the measured flag) and `ncei_events`.
> Hail table (from hail_days): Date; MESH at property/1/3/10 mi;
> reports/events panels. Render `definitions` as footnotes. Show "—" with
> tooltip "no cell ≥ threshold within range" for nulls. Read-only; do not
> alter database objects.

## Cost & size reality

- Compute: ~48 small byte-ranged slices/day daily ops; backfill uses the free
  Actions allowance exactly like your existing walkers.
- Storage: ANL cells ≥ 40 mph nationally average ~50–200k rows/day (spiky on
  hurricane/synoptic days); HRRR similar; rows are 26 bytes of payload each.
  4 years ≈ tens of GB worst case — if that exceeds your Supabase plan, the
  levers (in order): HRRR floor 45 mph, thin HRRR to every other cell (it's
  3 km vs 2.5), or partition + compress older years. Observation layers are
  negligible (a few million small rows total).

## What stays honest (and defensible)

- Every value labeled: analysis grid estimate vs model estimate vs measured.
- Sources never blended into a fake single number: the fused range widens
  when sources disagree, and `sources_active` shows how much support a day has.
- Estimated storm reports flagged and never treated as numeric truth; NCEI
  MG/EG kept.
- Durations declared as hourly-resolution analysis counts.

## Roadmap on top of this foundation

1. Station residual correction: nightly, compute (measured − analysis-at-
   station-cell) per station-day into a residuals table; the fusion RPC then
   applies a distance-weighted correction — the research docs' Option A,
   cheap because both sides are already stored.
2. Sub-hourly duration from HRRR 15-min max fields on flagged days.
3. Validation white paper: leave-one-station-out scoring straight from
   `hz_station_daily` vs `hz_wind_points` — the tables make it a SQL job.
