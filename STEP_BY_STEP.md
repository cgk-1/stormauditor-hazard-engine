# HAZARD ENGINE — Step-by-Step Implementation

Order matters. Steps 1–3 are GitHub, 4–5 are Lovable, 6 primes the data,
7 is the frontend, 8 verifies. ~45 minutes of your time; data backfills on
its own afterward. History depth: 2 years, stored permanently in Supabase
and rolled forward daily (each night adds a day, the purge drops day 731).

---

## Step 1 — Create the GitHub repo

1. Create a new **private** repo named `stormauditor-hazard-engine`.
2. Unzip `stormauditor-hazard-engine.zip` and push everything, keeping paths:
   ```
   hazard_wind_ingest.py
   hazard_obs_ingest.py
   hazard_backfill_walker.py
   HAZARD_ENGINE.md
   supabase/hazard_schema.sql
   .github/workflows/daily.yml
   .github/workflows/weekly.yml
   .github/workflows/backfill.yml
   ```
3. Confirm the three workflows appear under the repo's **Actions** tab
   (if Actions asks to be enabled, enable it).

## Step 2 — Invent the shared secret

This is a password you make up. It must end up in two places (GitHub +
Supabase) as the exact same string.

1. Generate one: `openssl rand -hex 24` (or any password generator).
   Example: `f3a91c07be2d5546a8e0d1c9724bb0e6621f884da3c57b12`
2. Save it. If your existing wind/hail feed repos already use an
   `INGEST_SECRET`, reuse that same string instead — then everything shares
   one secret.

## Step 3 — Add the three GitHub Actions secrets

Repo → Settings → Secrets and variables → Actions → New repository secret:

| Name | Value |
|---|---|
| `SUPABASE_URL` | your project URL, e.g. `https://xxxx.supabase.co` (in Lovable: project settings → Supabase/Cloud) |
| `SUPABASE_ANON_KEY` | the anon public key (same place) |
| `INGEST_SECRET` | the string from Step 2 |

## Step 4 — Create the schema (Lovable prompt #1)

Send Lovable this message, pasting the entire `supabase/hazard_schema.sql`
where indicated:

> Create a database migration from the SQL below. Keep every table name,
> function name, and function signature exactly as written — external GitHub
> Actions pipelines call these RPCs by name. One adaptation IS required:
> inside the function `property_hazard_report`, the hail section reads from
> `hail_points(date, lon, lat, v)`. Replace that one table reference (and its
> column names if different) with our actual MESH hail points table — the one
> our existing hail feed's `ingest_points` RPC writes to. Everything else
> stays verbatim. The hz_ tables and sa_config intentionally have RLS enabled
> with no policies (reads go only through the security-definer report
> function); do not add policies and do not flag this as an issue.
>
> [PASTE ALL OF hazard_schema.sql HERE]

If Lovable's security review complains about RLS-without-policies, reply:
"Intentional — leave as is."

## Step 5 — Store the secret in Supabase (Lovable prompt #2)

Swap in your Step 2 string between the quotes, then send exactly:

> Run this SQL exactly as written:
>
> insert into sa_config(key,value) values
> ('ingest_secret','PASTE_YOUR_SECRET_HERE')
> on conflict (key) do update set value = excluded.value;

## Step 6 — Prime the data (GitHub Actions, run manually once)

Repo → **Actions** tab, run each workflow with "Run workflow":

1. **hazard-weekly** — set `ncei_years` to `2024,2025,2026`.
   Loads station metadata, ~2.5 years of finalized NCEI Storm Events, and
   HURDAT2. ~10–15 min. Check the log ends without errors.
2. **hazard-daily** — run once with no inputs (does yesterday: both wind
   grids + station dailies + LSRs). ~5–10 min. The log should show lines like
   `2026-07-09 ANL: NNNN cells >= 40 mph` and `HRRR: NNNN cells`.
3. **Backfill the observation layers** — run **hazard-daily** twice more
   with these inputs (the obs job accepts ranges; the wind job just redoes
   yesterday, which is harmless):
   - ingest_date `20250101:20260708`
   - ingest_date `20240701:20241231`
   Each run's obs job takes ~10–20 min (one request per state network per
   range + national LSR pulls).
4. **hazard-backfill** — run once manually to start it, then leave it alone.
   It's scheduled every 2 hours and walks BOTH wind grids backward from
   yesterday toward 2 years, saving its cursor after every day. The most
   recent 12 months land within the first few days; full 2-year depth in
   about a week of free runners. Reports work immediately on whatever depth exists.

## Step 7 — Build the report page (Lovable prompt #4)

> Build a "Property Hazard Report" page. Flow: an address input and a
> "Generate report" button. On submit, geocode with the free US Census
> geocoder (GET
> https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address=URLENCODED&benchmark=Public_AR_Current&format=json
> — use result.addressMatches[0].coordinates: x is longitude, y is latitude;
> if no match, show "Address not found — include city, state, ZIP"). Then
> call the Supabase RPC `property_hazard_report` with p_lat and p_lon and
> render instantly (it returns in under a second):
>
> HEADLINE (from `summary`): two large cards — "Maximum estimated wind gust:
> {summary.max_wind.mph} mph on {date} (range {range_mph[0]}–{range_mph[1]},
> evidence grade {grade})" and "Maximum estimated hail:
> {summary.max_hail.inches}″ on {date} ({verdict})". If a summary field is
> null show "No qualifying events found in the period."
>
> WIND EVENTS table (from `wind_days`, newest first): Date | Estimated max
> gust (`estimate.max_gust_mph`, bold) | Range (`estimate.range_mph` as
> "lo–hi") | Grade | Hrs ≥40 / ≥58 (`hrs_ge_40`, `hrs_ge_58`) | 🌀 when
> `tropical`. Expandable row panel titled "How this estimate was built"
> showing: method `estimate.method`, background `estimate.background_mph`
> mph, residual correction `estimate.residual_correction_mph` mph from
> `estimate.residual_stations` station(s); then the inputs: analysis
> at/1/3/10 mi, HRRR at/1/3/10 mi, nearest measured gust (mph, station name,
> distance), storm_reports (show the measured flag distinctly), ncei_events.
>
> HAIL EVENTS table (from `hail_days`, newest first): Date | Estimated max
> hail (`estimate.max_hail_in`, bold) | Range | Verdict | Method. Expandable
> panel: MESH at/1/3/10 mi and storm_reports.
>
> Render the `methodology` object as footnotes at the bottom of the page.
> Read-only: do not create or modify any database objects.

## Step 8 — Verify end to end

1. In the app, run **400 W Church St, Orlando, FL 32801**.
2. With backfill only days old you'll at least see recent events; once the
   walker passes October 2024 you should see 2024-10-10 (Hurricane Milton)
   with an estimated gust in the ~70s mph, grade A/B, tropical flag, and the
   MCO measured gust in the evidence panel — matching the validated test.
3. Sanity-check speed: the RPC call should return in well under a second.
4. Watch the Actions tab for a few days: `hazard-daily` green every morning,
   `hazard-backfill` green every 2 hours until it prints
   "backfill COMPLETE".

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Worker logs say `forbidden` | Step 2 string differs between GitHub and Supabase — redo Step 5 with the exact GitHub value |
| `hz_wind_ingest 404` in logs | Wrong `SUPABASE_URL`, or Step 4 migration didn't run — confirm the RPCs exist |
| Report empty for known storms | Backfill hasn't reached that date yet — check the latest hazard-backfill log for the cursor date |
| Hail section empty | Step 4's one adaptation wasn't made — the function still references `hail_points`; re-prompt Lovable to point it at your real MESH table |
| Backfill seems slow | Each day = 48 downloads for CONUS; ~1 week to 2 yrs is expected. To prioritize wind only, set SOURCES=ANL via a workflow input, then rerun for HRRR later |

## After it's live (in order of value)

1. Nightly residual table + leave-one-station-out scoring (pure SQL over
   `hz_station_daily` vs `hz_wind_points`) → your published validation
   scorecard, the strongest sales asset.
2. Report PDF export with the `inputs` blocks — the immutable issued-report
   archive.
3. Retire the old wind feed once `hz_wind_points` backfills past its depth.
4. Extend to 4 years later if wanted: raise 730 to 1461 in hz_purge, the
   report default, and the walker, then reset the hzwind cursor — the walker
   simply keeps digging.
