-- Archive Phase 5, Stage 5: backfill 2021-10-01 -> 2024-07 in v4 (owner-approved 2026-10-07:
-- "yes do that workflow"; "fast track ... get moving on the archive plan now").
-- NOT APPLIED. Additive only: one new table + two new functions; no existing object changes.
-- Goes LIVE the moment it is applied; the only caller is the temporary workflow
-- stormauditor-hazard-engine/.github/workflows/stage5-backfill.yml (stage5_backfill.py).
-- The site, the engine and the nightly feeds never call these objects.
--
-- WHY A ROLL STEP IS NEEDED. hz_arch_roll_nightly(5,45) scans each source from
-- min(hz_arch_days.date) FORWARD. A backfilled day is OLDER than that minimum, so the nightly
-- roll never sees it: without this step its raw rows would stay in the raw tables forever and
-- re-bloat them (the space Phase 3/4 reclaimed). hz_p5_roll_v1 rolls each backfilled day right
-- after its ingest with the SAME function the nightly uses (hz_arch_roll(src, day, true):
-- pack -> exact verify -> list in hz_arch_days -> delete raw, one transaction).
--
-- HOLE SAFETY. Once a backfilled day D is listed, min(date) moves back to D and the nightly
-- roll would archive any unlisted day between D and the old minimum as an EMPTY day (0 = 0 is
-- "exact"). So D is rolled ONLY when D = min(date) - 1 for every source of the lane (strictly
-- contiguous, newest first). A failed day stops the lane; nothing older is ever rolled first.
--
-- Lanes (one workflow step each; the cursor key is hz_backfill 'p5_<lane>' = oldest finished day):
--   HAIL : src HAIL           days 2023-09-27 -> 2021-10-01 (archive starts 2023-09-28)
--   ANL  : src ANL + BGA      days 2024-07-22 -> 2021-10-01 (ANL archive starts 2024-07-24,
--                             BGA 2024-07-23: the lane cannot roll 07-22 until ANL 2024-07-23 is
--                             listed - an owner decision, see the go-live steps)
--   HRRR : src HRRR + BGH     days 2024-07-24 -> 2021-10-01 (archive starts 2024-07-25; nothing
--                             exists for HRRR/BGH/hz_station_bg HRRR/hz_hrrr_meta on 07-23..07-24)
--   OBS  : no archive         days 2024-07-21 -> 2021-10-01 (hz_station_daily/_peak start
--                             2024-07-22; hz_lsr starts 2024-07-21, so 07-21 runs without LSR)
--   NCEI : no archive         years 2023, 2022, 2021 (hz_storm_events starts 2024-01-02)
--
-- Objects
--   hz_backfill_log           one row per lane-day attempt (rolled / rolled_warn / refused / failed)
--   hz_p5_status_v1(secret)   read-only: db size, active backends, archive minimum per source,
--                             p5_* cursors. EXECUTE anon + service_role (secret-gated). Cheap.
--   hz_p5_roll_v1(secret, lane, day, run_id, max_db_gb default 20)
--                             guards + sanity + hz_arch_roll for every source of the lane + cursor,
--                             all in ONE subtransaction (a failure changes nothing but the log row).
--                             EXECUTE service_role ONLY: a big day's pack + verify takes several
--                             seconds, above anon's 3 s statement_timeout (service_role: 150 s).
--
-- Guards in hz_p5_roll_v1 (any failure = status 'refused' or 'failed', logged, nothing changed):
--   * secret; lane known; floor 2021-10-01 <= day <= the lane's ceiling (never a day >= the
--     existing data of that source; nothing >= 2024-07-23 except HRRR 07-23/07-24, which hold no
--     data of that source today)
--   * pg_database_size <= p_max_db_gb (default 20 GB, the §36 warn level)
--   * cursor: unset and day = ceiling, or cursor = day + 1 (contiguous, newest first)
--   * grid lanes: day not yet listed and day = min(hz_arch_days.date) - 1 for EVERY source
--   * sanity on the day's raw rows before the roll:
--       - ANL/BGA/HRRR/BGH must have rows (no empty day exists in two years); HAIL may be 0
--       - no NULL coordinates / values / states / window ends
--       - value bounds = the feeds' own validation: HAIL 0.50-8.00 in, ANL 40-300 mph,
--         HRRR 35-300 mph (FLOOR_MPH 35), BGA/BGH 5-300 mph
--       - HAIL on the MRMS lattice (x, y thousandths = 5 mod 10); no duplicate (x, y) in HAIL/ANL
--       - row count vs the median of the last 30 rolled days of that source: 0 or > 10x = refuse,
--         outside 0.2x-5x = rolled_warn (seasonal swings make a hard band a false-stop machine)
--   * OBS: hz_station_daily, hz_station_daily_v4 and hz_station_peak must have rows for the day
--   * NCEI: hz_storm_events (v3) for the year > 1000 rows and hz_storm_events_v4 has the same count
--
-- Rollback: drop function public.hz_p5_roll_v1(text, text, date, text, numeric);
--           drop function public.hz_p5_status_v1(text);
--           (keep hz_backfill_log; it is the audit trail of what was archived)
-- Undo of backfilled days (archive plan §7.3): delete the day's hz_arch_pt/hz_arch rows + its
-- hz_arch_days row in one transaction (tested in a rolled-back transaction first), oldest first.

create table if not exists public.hz_backfill_log (
  id         bigserial primary key,
  run_at     timestamptz not null default now(),
  run_id     text,
  lane       text not null,
  src        text,
  day        date not null,
  status     text not null check (status in ('rolled', 'rolled_warn', 'refused', 'failed', 'obs_ok', 'ncei_ok')),
  rows_raw   bigint,
  rows_arch  bigint,
  exact      boolean,
  arch_bytes bigint,
  ms         integer,
  db_bytes   bigint,
  detail     jsonb,
  error      text
);
create index if not exists hz_backfill_log_src_id on public.hz_backfill_log (src, id desc);
create index if not exists hz_backfill_log_lane_day on public.hz_backfill_log (lane, day);
alter table public.hz_backfill_log enable row level security;
revoke all on public.hz_backfill_log from anon, authenticated, public;
revoke all on sequence public.hz_backfill_log_id_seq from anon, authenticated, public;

-- ======================================================================== status (read-only)
create or replace function public.hz_p5_status_v1(p_secret text)
returns jsonb language plpgsql stable security definer set search_path to 'public' as $$
declare out jsonb;
begin
  if not (sa_secret_ok(p_secret)
          or (p_secret is not distinct from (select value from app_config where key = 'ingest_secret')
              and p_secret <> 'CHANGE-ME')) then
    raise exception 'unauthorized';
  end if;
  select jsonb_build_object(
    'db_bytes', pg_database_size(current_database()),
    'active', (select count(*) from pg_stat_activity
               where state = 'active' and backend_type = 'client backend' and pid <> pg_backend_pid()),
    'arch_min', (select jsonb_object_agg(s, (select min(date) from hz_arch_days a where a.src = s))
                 from unnest(array['HAIL','ANL','BGA','HRRR','BGH']) s),
    'cursors', coalesce((select jsonb_object_agg(key, value) from hz_backfill where key like 'p5\_%'), '{}'::jsonb),
    'last_log', (select jsonb_agg(x) from (select id, run_at, lane, src, day, status, error
                                           from hz_backfill_log order by id desc limit 5) x),
    'now', now()) into out;
  return out;
end $$;

-- ======================================================================== roll one lane-day
create or replace function public.hz_p5_roll_v1(p_secret text, p_lane text, p_day date,
                                                p_run_id text default null, p_max_db_gb numeric default 20)
returns jsonb language plpgsql security definer set search_path to 'public' as $$
declare
  c_floor  date := date '2021-10-01';
  ceil_day date;
  srcs     text[];
  s        text;
  cur      text;
  m        date;
  db       bigint := pg_database_size(current_database());
  t0       timestamptz := clock_timestamp();
  n bigint; nbad bigint; ndup bigint; nlat bigint; med numeric; nmed int;
  rv jsonb; ab bigint; st text;
  res      jsonb := '[]'::jsonb;
  warns    jsonb := '[]'::jsonb;
  reason   text;
  lo numeric; hi numeric;
begin
  if not (sa_secret_ok(p_secret)
          or (p_secret is not distinct from (select value from app_config where key = 'ingest_secret')
              and p_secret <> 'CHANGE-ME')) then
    raise exception 'unauthorized';
  end if;
  p_lane := upper(coalesce(p_lane, ''));
  ceil_day := case p_lane when 'HAIL' then date '2023-09-27' when 'ANL' then date '2024-07-22'
                          when 'HRRR' then date '2024-07-24' when 'OBS' then date '2024-07-21'
                          when 'NCEI' then date '2023-01-01' end;
  srcs := case p_lane when 'HAIL' then array['HAIL'] when 'ANL' then array['ANL','BGA']
                      when 'HRRR' then array['HRRR','BGH'] else array[]::text[] end;
  if p_lane = 'NCEI' then c_floor := date '2021-01-01'; end if;       -- whole NCEI years 2021-2023
  perform pg_advisory_xact_lock(hashtext('hz_p5_roll_v1'));

  -- ---------------------------------------------------------------- refusals (nothing touched)
  if ceil_day is null then
    reason := format('unknown lane %s', p_lane);
  elsif p_day is null or p_day < c_floor or p_day > ceil_day
        or (p_lane = 'NCEI' and (extract(month from p_day) <> 1 or extract(day from p_day) <> 1)) then
    reason := format('day %s outside the lane window %s..%s', p_day, c_floor, ceil_day);
  elsif db > p_max_db_gb * 1024 * 1024 * 1024 then
    reason := format('database %s bytes > %s GB guard', db, p_max_db_gb);
  else
    select value into cur from hz_backfill where key = 'p5_' || lower(p_lane);
    if cur is null and p_day <> ceil_day then
      reason := format('cursor p5_%s unset: the first day must be %s', lower(p_lane), ceil_day);
    elsif cur is not null and cur::date <> (case when p_lane = 'NCEI' then p_day + interval '1 year'
                                                 else p_day + 1 end)::date then
      reason := format('cursor p5_%s = %s: day %s is not the next one (contiguous, newest first)',
                       lower(p_lane), cur, p_day);
    end if;
  end if;
  if reason is null then
    foreach s in array srcs loop
      if exists (select 1 from hz_arch_days where src = s and date = p_day) then
        reason := format('%s %s is already archived', s, p_day); exit;
      end if;
      select min(date) into m from hz_arch_days where src = s;
      if m is distinct from p_day + 1 then
        reason := format('%s: archive starts %s, so %s is not contiguous (would leave a hole the nightly '
                         'roll archives as an empty day)', s, m, p_day); exit;
      end if;
    end loop;
  end if;
  if reason is not null then
    insert into hz_backfill_log(run_id, lane, day, status, ms, db_bytes, error)
    values (p_run_id, p_lane, coalesce(p_day, c_floor), 'refused',
            (extract(epoch from clock_timestamp() - t0) * 1000)::int, db, reason);
    return jsonb_build_object('ok', false, 'status', 'refused', 'lane', p_lane, 'day', p_day, 'error', reason);
  end if;

  -- ---------------------------------------------------------------- sanity + roll (one subtransaction)
  begin
    if p_lane = 'OBS' then
      if not exists (select 1 from hz_station_daily where date = p_day) then
        raise exception 'OBS %: no hz_station_daily rows', p_day;
      end if;
      if not exists (select 1 from hz_station_daily_v4 where date = p_day) then
        raise exception 'OBS %: no hz_station_daily_v4 rows', p_day;
      end if;
      if not exists (select 1 from hz_station_peak where date = p_day) then
        raise exception 'OBS %: no hz_station_peak rows', p_day;
      end if;
      res := jsonb_build_array(jsonb_build_object(
        'daily', (select count(*) from hz_station_daily where date = p_day),
        'daily_v4', (select count(*) from hz_station_daily_v4 where date = p_day),
        'lsr', (select count(*) from hz_lsr where date = p_day),
        'lsr_v4', (select count(*) from hz_lsr_v4 where date = p_day)));
      insert into hz_backfill_log(run_id, lane, day, status, ms, db_bytes, detail)
      values (p_run_id, p_lane, p_day, 'obs_ok', (extract(epoch from clock_timestamp() - t0) * 1000)::int, db, res->0);

    elsif p_lane = 'NCEI' then
      select count(*) into n from hz_storm_events
       where date >= p_day and date < (p_day + interval '1 year')::date;
      select count(*) into nbad from hz_storm_events_v4 where ncei_year = extract(year from p_day)::int;
      if n <= 1000 then
        raise exception 'NCEI %: only % hz_storm_events rows', extract(year from p_day), n;
      end if;
      if nbad <> n then
        raise exception 'NCEI %: v4 has % rows, v3 has %', extract(year from p_day), nbad, n;
      end if;
      res := jsonb_build_array(jsonb_build_object('v3', n, 'v4', nbad));
      insert into hz_backfill_log(run_id, lane, day, status, rows_raw, ms, db_bytes, detail)
      values (p_run_id, p_lane, p_day, 'ncei_ok', n, (extract(epoch from clock_timestamp() - t0) * 1000)::int, db, res->0);

    else
      foreach s in array srcs loop
        st := 'rolled';
        if s = 'HAIL' then
          select count(*),
                 count(*) filter (where geom is null or in_val is null or state is null or window_end_utc is null
                                  or in_val < 0.50 or in_val > 8.00),
                 count(*) - count(distinct (round(st_x(geom) * 1000), round(st_y(geom) * 1000))),
                 count(*) filter (where abs(round(st_x(geom) * 1000)::bigint) % 10 <> 5
                                     or abs(round(st_y(geom) * 1000)::bigint) % 10 <> 5)
            into n, nbad, ndup, nlat from hail_points where valid_date = p_day;
          lo := 0.50; hi := 8.00;
        elsif s = 'ANL' then
          select count(*),
                 count(*) filter (where geom is null or mph_val is null or state is null
                                  or mph_val < 40 or mph_val > 300),
                 count(*) - count(distinct (round(st_x(geom) * 1000), round(st_y(geom) * 1000))), 0
            into n, nbad, ndup, nlat from wind_points where valid_date = p_day;
          lo := 40; hi := 300;
        elsif s = 'HRRR' then
          select count(*),
                 count(*) filter (where lon is null or lat is null or v is null or state is null
                                  or d40 is null or d58 is null or v < 35 or v > 300),
                 0, 0
            into n, nbad, ndup, nlat from hz_hrrr_points where date = p_day;
          lo := 35; hi := 300;
        else
          select count(*),
                 count(*) filter (where lon is null or lat is null or v is null or v < 5 or v > 300), 0, 0
            into n, nbad, ndup, nlat from hz_bg_coarse
           where src = case s when 'BGA' then 'ANL' else 'HRRR' end and date = p_day;
          lo := 5; hi := 300;
        end if;
        if n = 0 and s <> 'HAIL' then
          raise exception '% %: no raw rows (an empty % day never occurs)', s, p_day, s;
        end if;
        if nbad > 0 then
          raise exception '% %: % row(s) with NULLs or values outside %-%', s, p_day, nbad, lo, hi;
        end if;
        if ndup > 0 then
          raise exception '% %: % duplicate (x, y) cell(s)', s, p_day, ndup;
        end if;
        if nlat > 0 then
          raise exception '% %: % point(s) off the MRMS 0.01 deg lattice', s, p_day, nlat;
        end if;
        select percentile_cont(0.5) within group (order by rows_raw), count(*) into med, nmed
          from (select rows_raw from hz_backfill_log where src = s and status in ('rolled', 'rolled_warn')
                order by id desc limit 30) z;
        if nmed >= 10 and med > 0 and s <> 'HAIL' then
          if n > 10 * med then
            raise exception '% %: % rows > 10x the median % of the last % rolled days', s, p_day, n, med, nmed;
          elsif n < 0.2 * med or n > 5 * med then
            st := 'rolled_warn';
            warns := warns || jsonb_build_object('src', s, 'rows', n, 'median', med);
          end if;
        end if;

        rv := hz_arch_roll(s, p_day, true);           -- pack -> exact verify (raises) -> list -> delete raw
        if coalesce(rv->>'status', '') <> 'archived' or not coalesce((rv->>'exact')::boolean, false) then
          raise exception '% %: roll returned %', s, p_day, rv;
        end if;
        if s in ('ANL', 'HAIL') then
          select coalesce(sum(pg_column_size(a.*)), 0) into ab from hz_arch_pt a where a.src = s and a.date = p_day;
        else
          select coalesce(sum(pg_column_size(a.*)), 0) into ab from hz_arch a where a.src = s and a.date = p_day;
        end if;
        insert into hz_backfill_log(run_id, lane, src, day, status, rows_raw, rows_arch, exact, arch_bytes, ms, db_bytes, detail)
        values (p_run_id, p_lane, s, p_day, st, n, (rv->>'arch')::bigint, true, ab,
                (extract(epoch from clock_timestamp() - t0) * 1000)::int, db, rv);
        res := res || jsonb_build_object('src', s, 'status', st, 'rows', n, 'arch_bytes', ab);
      end loop;
    end if;

    insert into hz_backfill(key, value)
    values ('p5_' || lower(p_lane), case when p_lane = 'NCEI' then extract(year from p_day)::int::text || '-01-01'
                                         else p_day::text end)
    on conflict (key) do update set value = excluded.value;
  exception when others then
    insert into hz_backfill_log(run_id, lane, day, status, ms, db_bytes, error)
    values (p_run_id, p_lane, p_day, 'failed', (extract(epoch from clock_timestamp() - t0) * 1000)::int, db, sqlerrm);
    return jsonb_build_object('ok', false, 'status', 'failed', 'lane', p_lane, 'day', p_day, 'error', sqlerrm);
  end;
  return jsonb_build_object('ok', true, 'status', case when jsonb_array_length(warns) > 0 then 'rolled_warn' else 'ok' end,
                            'lane', p_lane, 'day', p_day, 'sources', res, 'warnings', warns,
                            'ms', (extract(epoch from clock_timestamp() - t0) * 1000)::int, 'db_bytes', db);
end $$;

revoke all on function public.hz_p5_status_v1(text) from public, anon, authenticated;
revoke all on function public.hz_p5_roll_v1(text, text, date, text, numeric) from public, anon, authenticated;
grant execute on function public.hz_p5_status_v1(text) to anon, service_role;
grant execute on function public.hz_p5_roll_v1(text, text, date, text, numeric) to service_role;

-- Post-apply checks (SELECT only):
--   select proname, prosecdef, proacl from pg_proc where proname in ('hz_p5_status_v1', 'hz_p5_roll_v1');
--     -- status: anon + service_role; roll: service_role only; both prosecdef = t
--   select has_function_privilege('anon', 'hz_p5_roll_v1(text,text,date,text,numeric)', 'execute');   -- f
--   select relrowsecurity from pg_class where relname = 'hz_backfill_log';                           -- t
--   select count(*) from hz_backfill_log;                                                            -- 0
