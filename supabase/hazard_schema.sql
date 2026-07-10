-- ============================================================================
-- StormAuditor HAZARD ENGINE — schema + fusion RPC  (Lovable migration)
--
-- Layers stored (all precomputed nationally by the GitHub pipelines):
--   hz_wind_points     ANL (URMA/RTMA) + HRRR daily max gust cells >= 40 mph,
--                      with per-cell hours >= 40 / >= 58 mph
--   hz_stations        ASOS/AWOS station metadata
--   hz_station_daily   per-station daily peak measured gust (>= 20 mph)
--   hz_lsr             NWS Local Storm Reports (wind + hail, measured flag)
--   hz_storm_events    finalized NCEI Storm Events (the citable record)
--   hz_hurdat          NHC best-track points (tropical-day flagging)
--   (hail cells come from your existing MESH feed table; see fusion RPC note)
--
-- The fusion RPC property_hazard_report(lat, lon, start, end) returns the
-- full multi-source report in one sub-second call: per storm day, at-property
-- + 1/3/10-mile values from BOTH wind sources, durations, nearest measured
-- gust, storm reports, finalized events, tropical flag, MESH hail rings, and
-- a fused best-estimate + range + evidence grade.
-- ============================================================================

-- shared secret check (reuse if sa_config/sa_secret_ok already exist)
create table if not exists sa_config (key text primary key, value text not null);
alter table sa_config enable row level security;
create or replace function sa_secret_ok(p_secret text) returns boolean
language sql stable security definer set search_path = public as $$
  select exists(select 1 from sa_config
                where key='ingest_secret' and value=p_secret);
$$;

-- ------------------------------------------------------------------ tables
create table if not exists hz_wind_points (
  date date not null,
  src  text not null check (src in ('ANL','HRRR')),
  lon  real not null,
  lat  real not null,
  v    smallint not null,          -- daily max gust, mph
  d40  smallint not null default 0,-- hours >= 40 mph
  d58  smallint not null default 0,-- hours >= 58 mph
  primary key (date, src, lon, lat)
);
create index if not exists hz_wind_geo_idx on hz_wind_points (lon, lat, date);

create table if not exists hz_wind_meta (
  date date, src text, detail text, hours int,
  primary key (date, src)
);

create table if not exists hz_stations (
  stid text primary key, name text, state text,
  lat real, lon real, network text
);
create index if not exists hz_stations_geo_idx on hz_stations (lon, lat);

create table if not exists hz_station_daily (
  stid text, date date, gust_mph real,
  primary key (stid, date)
);
create index if not exists hz_station_daily_date_idx on hz_station_daily (date);

create table if not exists hz_lsr (
  id bigint generated always as identity primary key,
  time_utc text, date date, kind text check (kind in ('wind','hail')),
  type text, mag text, lat real, lon real,
  city text, state text, source text, measured boolean
);
create index if not exists hz_lsr_geo_idx on hz_lsr (kind, lon, lat, date);

create table if not exists hz_storm_events (
  event_id text primary key, kind text, event_type text,
  begin_utc text, date date, lat real, lon real,
  magnitude text, mag_type text, state text, cz_name text
);
create index if not exists hz_se_geo_idx on hz_storm_events (kind, lon, lat, date);

create table if not exists hz_hurdat (
  storm_id text, name text, time_utc text, date date,
  status text, lat real, lon real, wind_kt int,
  primary key (storm_id, time_utc)
);
create index if not exists hz_hurdat_date_idx on hz_hurdat (date);

alter table hz_wind_points   enable row level security;
alter table hz_wind_meta     enable row level security;
alter table hz_stations      enable row level security;
alter table hz_station_daily enable row level security;
alter table hz_lsr           enable row level security;
alter table hz_storm_events  enable row level security;
alter table hz_hurdat        enable row level security;
-- reads happen only through the security-definer report RPC; no policies.

-- ------------------------------------------------------------- ingest RPCs
create or replace function hz_wind_ingest(p_secret text, p_date date,
  p_src text, p_detail text, p_hours int, p_points jsonb, p_append boolean)
returns void language plpgsql security definer set search_path=public as $$
begin
  if not sa_secret_ok(p_secret) then raise exception 'forbidden'; end if;
  if not p_append then
    delete from hz_wind_points where date=p_date and src=p_src;
    insert into hz_wind_meta values (p_date, p_src, p_detail, p_hours)
      on conflict (date,src) do update
      set detail=excluded.detail, hours=excluded.hours;
  end if;
  insert into hz_wind_points
  select p_date, p_src, (p->>'lon')::real, (p->>'lat')::real,
         (p->>'v')::smallint, (p->>'d40')::smallint, (p->>'d58')::smallint
  from jsonb_array_elements(p_points) p
  on conflict do nothing;
end $$;

create or replace function hz_stations_ingest(p_secret text, p_rows jsonb,
  p_append boolean)
returns void language plpgsql security definer set search_path=public as $$
begin
  if not sa_secret_ok(p_secret) then raise exception 'forbidden'; end if;
  insert into hz_stations
  select r->>'stid', r->>'name', r->>'state',
         (r->>'lat')::real, (r->>'lon')::real, r->>'network'
  from jsonb_array_elements(p_rows) r
  on conflict (stid) do update set name=excluded.name, lat=excluded.lat,
    lon=excluded.lon, state=excluded.state, network=excluded.network;
end $$;

create or replace function hz_station_daily_ingest(p_secret text,
  p_d0 date, p_d1 date, p_rows jsonb, p_append boolean)
returns void language plpgsql security definer set search_path=public as $$
begin
  if not sa_secret_ok(p_secret) then raise exception 'forbidden'; end if;
  if not p_append then
    delete from hz_station_daily where date between p_d0 and p_d1;
  end if;
  insert into hz_station_daily
  select r->>'stid', (r->>'date')::date, (r->>'gust_mph')::real
  from jsonb_array_elements(p_rows) r
  on conflict (stid,date) do update set gust_mph=excluded.gust_mph;
end $$;

create or replace function hz_lsr_ingest(p_secret text, p_d0 date, p_d1 date,
  p_rows jsonb, p_append boolean)
returns void language plpgsql security definer set search_path=public as $$
begin
  if not sa_secret_ok(p_secret) then raise exception 'forbidden'; end if;
  if not p_append then
    delete from hz_lsr where date between p_d0 and p_d1;
  end if;
  insert into hz_lsr (time_utc, date, kind, type, mag, lat, lon, city, state,
                      source, measured)
  select r->>'time_utc', to_date(left(r->>'time_utc',8),'YYYYMMDD'),
         r->>'kind', r->>'type', r->>'mag',
         (r->>'lat')::real, (r->>'lon')::real,
         r->>'city', r->>'state', r->>'source', (r->>'measured')::boolean
  from jsonb_array_elements(p_rows) r;
end $$;

create or replace function hz_storm_events_ingest(p_secret text, p_year int,
  p_rows jsonb, p_append boolean)
returns void language plpgsql security definer set search_path=public as $$
begin
  if not sa_secret_ok(p_secret) then raise exception 'forbidden'; end if;
  if not p_append then
    delete from hz_storm_events where extract(year from date)=p_year;
  end if;
  insert into hz_storm_events
  select r->>'event_id', r->>'kind', r->>'event_type', r->>'begin_utc',
         to_date(left(r->>'begin_utc',8),'YYYYMMDD'),
         (r->>'lat')::real, (r->>'lon')::real,
         r->>'magnitude', r->>'mag_type', r->>'state', r->>'cz_name'
  from jsonb_array_elements(p_rows) r
  on conflict (event_id) do nothing;
end $$;

create or replace function hz_hurdat_ingest(p_secret text, p_rows jsonb,
  p_append boolean)
returns void language plpgsql security definer set search_path=public as $$
begin
  if not sa_secret_ok(p_secret) then raise exception 'forbidden'; end if;
  if not p_append then delete from hz_hurdat; end if;
  insert into hz_hurdat
  select r->>'storm_id', r->>'name', r->>'time_utc',
         to_date(left(r->>'time_utc',8),'YYYYMMDD'),
         r->>'status', (r->>'lat')::real, (r->>'lon')::real,
         nullif(r->>'wind_kt','')::int
  from jsonb_array_elements(p_rows) r
  on conflict do nothing;
end $$;

create or replace function hz_purge(p_secret text)
returns void language plpgsql security definer set search_path=public as $$
begin
  if not sa_secret_ok(p_secret) then raise exception 'forbidden'; end if;
  delete from hz_wind_points   where date < current_date - 730;
  delete from hz_station_daily where date < current_date - 730;
  delete from hz_lsr           where date < current_date - 730;
  delete from hz_storm_events  where date < current_date - 730;
end $$;

-- ============================================================================
-- THE PRODUCT: property_hazard_report — proprietary per-address estimator
--
-- Returns, instantly, for any lat/lon:
--   summary.max_wind    ONE per-address maximum estimated gust (mph) over the
--                       period, with date, range, grade   (SAWE-1 algorithm)
--   summary.max_hail    ONE per-address maximum estimated hail size (in)
--                       with date and verdict             (SAHE-1 algorithm)
--   wind_days[]         every event day with its SAWE-1 estimate + all inputs
--   hail_days[]         every event day with its SAHE-1 estimate + all inputs
--
-- SAWE-1 (StormAuditor Wind Estimate, v1) — deterministic, documented:
--   1 BACKGROUND  U = analysis at-property cell (URMA/RTMA, 2.5 km).
--       If the HRRR cell exceeds it, U += 0.5*(HRRR-analysis): the analysis
--       of record smooths narrow convective swaths that radar-assimilating
--       HRRR resolves, so upward HRRR disagreement is partially credited;
--       downward disagreement is not (the analysis assimilates observations).
--       If no analysis cell >= 40 mph within 2 mi, U = HRRR cell.
--   2 RESIDUAL CORRECTION  For stations <= 30 mi with a same-day measured
--       peak gust AND a stored analysis cell within 3 mi of the station,
--       residual r_i = measured_i - analysis_i. Correction =
--       sum(w_i*r_i)/sum(w_i), w_i = 1/(1+d_i/10)^2, clamped to ±15 mph.
--       This transfers the locally observed analysis error to the address.
--   3 BOUNDS  Estimate clamped inside
--       [ least(analysis_at, HRRR_at) , greatest(1-mi maxima, measured<=10mi) ].
--   4 OUTPUT  best estimate (integer mph), range, grade
--       A: measured <=10 mi agrees ±20% (or tropical + measured)
--       B: measured <=25 mi, or measured LSR, or measured NCEI event
--       C: model/analysis-only
--
-- SAHE-1 (StormAuditor Hail Estimate, v1):
--   1 BASE  MESH at-property 1 km cell (radar-estimated max hail size).
--   2 ADJACENT-CELL INFERENCE  If no at-property cell, estimate =
--       0.85 * max within 1 mi (hail cores are sub-mile; adjacent-cell
--       gradient discount, labeled as inference).
--   3 REPORT RECONCILIATION  Ground reports <= 3 mi set a floor of
--       min(report size, max MESH within 3 mi) — reports confirm occurrence
--       but a report 2+ mi away is not claimed at the address beyond what
--       radar supports.
--   4 VERDICT  confirmed_nearby / likely / possible per documented rules.
--
-- Nothing is claimed as measured at the property; every estimate carries its
-- inputs, method fields, and bounds so the number is reproducible by hand.
-- ============================================================================
create or replace function property_hazard_report(
  p_lat double precision, p_lon double precision,
  p_start date default null, p_end date default null
) returns jsonb
language plpgsql security definer set search_path = public as $$
declare
  s date := coalesce(p_start, current_date - 730);
  e date := coalesce(p_end,   current_date - 1);
  dla double precision := 10.3/69.0;
  dlo double precision := 10.3/(69.0*cos(radians(p_lat)));
  wind jsonb; hail jsonb; wsum jsonb; hsum jsonb;
begin
  -- ---- wind cells within 10 mi
  create temp table _w on commit drop as
    select date, src, v, d40, d58,
           2*3958.8*asin(sqrt( sin(radians(lat-p_lat)/2)^2
             + cos(radians(p_lat))*cos(radians(lat))
             * sin(radians(lon-p_lon)/2)^2 )) as mi
    from hz_wind_points
    where lon between p_lon-dlo and p_lon+dlo
      and lat between p_lat-dla and p_lat+dla
      and date between s and e;
  delete from _w where mi > 10.0;

  -- ---- stations within 30 mi, with same-day analysis value at the station
  create temp table _stn on commit drop as
    select d.date, d.gust_mph, st.stid, st.name,
      round((2*3958.8*asin(sqrt( sin(radians(st.lat-p_lat)/2)^2
        + cos(radians(p_lat))*cos(radians(st.lat))
        * sin(radians(st.lon-p_lon)/2)^2 )))::numeric,1) as mi,
      (select w.v::real from hz_wind_points w
        where w.src='ANL' and w.date=d.date
          and w.lon between st.lon-0.06 and st.lon+0.06
          and w.lat between st.lat-0.05 and st.lat+0.05
        order by (w.lon-st.lon)^2 + (w.lat-st.lat)^2
        limit 1) as anl_at_stn
    from hz_station_daily d join hz_stations st using (stid)
    where st.lon between p_lon-3.2*dlo and p_lon+3.2*dlo
      and st.lat between p_lat-3.2*dla and p_lat+3.2*dla
      and d.date between s and e
      and 2*3958.8*asin(sqrt( sin(radians(st.lat-p_lat)/2)^2
        + cos(radians(p_lat))*cos(radians(st.lat))
        * sin(radians(st.lon-p_lon)/2)^2 )) <= 30;

  with
  at_cell as (
    select distinct on (date, src) date, src, v, d40, d58, mi
    from _w order by date, src, mi),
  rings as (
    select date, src,
      max(v) filter (where mi<=1)  r1,
      max(v) filter (where mi<=3)  r3,
      max(v) filter (where mi<=10) r10
    from _w group by date, src),
  layer as (
    select r.date, r.src, r.r1, r.r3, r.r10,
      case when a.mi<=2.0 then a.v end   as at_v,
      round(a.mi::numeric,1)             as at_mi,
      case when a.mi<=2.0 then a.d40 end as d40,
      case when a.mi<=2.0 then a.d58 end as d58
    from rings r join at_cell a using (date, src)),
  beststn as (
    select distinct on (date) date, gust_mph, stid, name, mi
    from _stn order by date, gust_mph desc),
  resid as (       -- distance-weighted mean analysis residual, clamped ±15
    select date,
      greatest(-15, least(15,
        sum((gust_mph - anl_at_stn) / power(1 + mi/10.0, 2))
        / nullif(sum(1 / power(1 + mi/10.0, 2)), 0)))::numeric(5,1) as corr,
      count(*) as n_res
    from _stn where anl_at_stn is not null
    group by date),
  rpt as (
    select date, kind, jsonb_agg(jsonb_build_object(
        'time_utc',time_utc,'type',type,'mag',mag,'city',city,
        'source',source,'measured',measured,
        'dist_mi', round((2*3958.8*asin(sqrt( sin(radians(lat-p_lat)/2)^2
          + cos(radians(p_lat))*cos(radians(lat))
          * sin(radians(lon-p_lon)/2)^2 )))::numeric,1))
        order by measured desc) as reports,
      bool_or(measured) as any_measured
    from hz_lsr
    where lon between p_lon-1.5*dlo and p_lon+1.5*dlo
      and lat between p_lat-1.5*dla and p_lat+1.5*dla
      and date between s and e
      and 2*3958.8*asin(sqrt( sin(radians(lat-p_lat)/2)^2
        + cos(radians(p_lat))*cos(radians(lat))
        * sin(radians(lon-p_lon)/2)^2 )) <= 15
    group by date, kind),
  sev as (
    select date, kind, jsonb_agg(jsonb_build_object(
        'event_type',event_type,'magnitude',magnitude,'mag_type',mag_type,
        'cz_name',cz_name,'event_id',event_id)) as events,
      bool_or(mag_type='MG') as any_measured
    from hz_storm_events
    where lon between p_lon-1.5*dlo and p_lon+1.5*dlo
      and lat between p_lat-1.5*dla and p_lat+1.5*dla
      and date between s and e
      and 2*3958.8*asin(sqrt( sin(radians(lat-p_lat)/2)^2
        + cos(radians(p_lat))*cos(radians(lat))
        * sin(radians(lon-p_lon)/2)^2 )) <= 15
    group by date, kind),
  trop as (
    select distinct d from (
      select date d from hz_hurdat where 2*3958.8*asin(sqrt(
        sin(radians(lat-p_lat)/2)^2 + cos(radians(p_lat))*cos(radians(lat))
        * sin(radians(lon-p_lon)/2)^2 )) <= 250
      union all
      select date-1 from hz_hurdat where 2*3958.8*asin(sqrt(
        sin(radians(lat-p_lat)/2)^2 + cos(radians(p_lat))*cos(radians(lat))
        * sin(radians(lon-p_lon)/2)^2 )) <= 250) x
    where d between s and e),
  wdates as (
    select date from layer
    union select date from beststn where gust_mph >= 40
    union select date from rpt where kind='wind'),
  calc as (
    select w.date,
      a.at_v anl_at, a.at_mi anl_mi, a.r1 anl_r1, a.r3 anl_r3,
      a.r10 anl_r10, a.d40, a.d58,
      h.at_v hr_at, h.r1 hr_r1, h.r3 hr_r3, h.r10 hr_r10,
      st.gust_mph stn_mph, st.stid, st.name stn_name, st.mi stn_mi,
      rs.corr, coalesce(rs.n_res,0) n_res,
      rw.reports wind_reports, coalesce(rw.any_measured,false) lsr_m,
      sw.events wind_events, coalesce(sw.any_measured,false) se_m,
      (w.date in (select d from trop)) tropical,
      -- SAWE-1 step 1: background
      case
        when a.at_v is not null and h.at_v is not null and h.at_v > a.at_v
          then a.at_v + 0.5*(h.at_v - a.at_v)
        when a.at_v is not null then a.at_v::numeric
        else h.at_v::numeric end as base_u
    from wdates w
    left join layer a on a.date=w.date and a.src='ANL'
    left join layer h on h.date=w.date and h.src='HRRR'
    left join beststn st on st.date=w.date
    left join resid rs on rs.date=w.date
    left join rpt rw on rw.date=w.date and rw.kind='wind'
    left join sev sw on sw.date=w.date and sw.kind='wind'),
  est as (
    select *,
      -- SAWE-1 steps 2+3: residual correction, then clamp inside bounds
      greatest(1,
        least(
          greatest(coalesce(anl_r1,0), coalesce(hr_r1,0),
                   coalesce(base_u,0),
                   case when stn_mi<=10 then coalesce(stn_mph,0) else 0 end),
          greatest(
            least(coalesce(anl_at, hr_at), coalesce(hr_at, anl_at)),
            base_u + coalesce(corr, 0))))::int as sawe
    from calc where base_u is not null)
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'date', date,
      'estimate', jsonb_build_object(
        'max_gust_mph', sawe,
        'range_mph', jsonb_build_array(
           least(coalesce(anl_at,hr_at), coalesce(hr_at,anl_at), sawe),
           greatest(coalesce(anl_r1,0), coalesce(hr_r1,0), sawe,
             case when stn_mi<=10 then coalesce(stn_mph,0) else 0 end)),
        'grade', case
          when stn_mph is not null and stn_mi<=10
               and abs(stn_mph - sawe) <= 0.2*greatest(stn_mph, sawe) then 'A'
          when tropical and stn_mph is not null then 'A'
          when (stn_mph is not null and stn_mi<=25) or lsr_m or se_m then 'B'
          else 'C' end,
        'method', 'SAWE-1',
        'background_mph', round(base_u,1),
        'residual_correction_mph', coalesce(corr,0),
        'residual_stations', n_res),
      'hrs_ge_40', d40, 'hrs_ge_58', d58, 'tropical', tropical,
      'inputs', jsonb_build_object(
        'analysis', jsonb_build_object('at_mph',anl_at,'nearest_cell_mi',
            anl_mi,'r1_mph',anl_r1,'r3_mph',anl_r3,'r10_mph',anl_r10),
        'hrrr', jsonb_build_object('at_mph',hr_at,'r1_mph',hr_r1,
            'r3_mph',hr_r3,'r10_mph',hr_r10),
        'nearest_measured_gust', case when stn_mph is not null then
            jsonb_build_object('mph',stn_mph,'station',stid,'name',stn_name,
                               'dist_mi',stn_mi) end,
        'storm_reports', coalesce(wind_reports,'[]'::jsonb),
        'ncei_events', coalesce(wind_events,'[]'::jsonb))
    ) order by date desc), '[]'::jsonb),
    (select jsonb_build_object('mph', sawe, 'date', date,
        'range_mph', jsonb_build_array(
           least(coalesce(anl_at,hr_at), coalesce(hr_at,anl_at), sawe),
           greatest(coalesce(anl_r1,0), coalesce(hr_r1,0), sawe,
             case when stn_mi<=10 then coalesce(stn_mph,0) else 0 end)),
        'grade', case
          when stn_mph is not null and stn_mi<=10
               and abs(stn_mph - sawe) <= 0.2*greatest(stn_mph, sawe) then 'A'
          when tropical and stn_mph is not null then 'A'
          when (stn_mph is not null and stn_mi<=25) or lsr_m or se_m then 'B'
          else 'C' end)
     from est order by sawe desc, date desc limit 1)
  into wind, wsum from est;

  -- ------------------------------ hail: SAHE-1 --------------------------
  -- NOTE FOR LOVABLE: replace hail_points(date,lon,lat,v) with the actual
  -- table/columns your existing MESH feed writes.
  with hp as (
    select date, v,
      2*3958.8*asin(sqrt( sin(radians(lat-p_lat)/2)^2
        + cos(radians(p_lat))*cos(radians(lat))
        * sin(radians(lon-p_lon)/2)^2 )) as mi
    from hail_points
    where lon between p_lon-dlo and p_lon+dlo
      and lat between p_lat-dla and p_lat+dla
      and date between s and e),
  hagg as (
    select date,
      (array_agg(v order by mi))[1] at_v, min(mi) at_mi,
      max(v) filter (where mi<=1) r1, max(v) filter (where mi<=3) r3,
      max(v) filter (where mi<=10) r10
    from hp where mi<=10 group by date),
  hrpt as (
    select date,
      max(nullif(regexp_replace(mag,'[^0-9.]','','g'),'')::numeric)
        filter (where 2*3958.8*asin(sqrt( sin(radians(lat-p_lat)/2)^2
          + cos(radians(p_lat))*cos(radians(lat))
          * sin(radians(lon-p_lon)/2)^2 )) <= 3) as sz3,
      min(2*3958.8*asin(sqrt( sin(radians(lat-p_lat)/2)^2
          + cos(radians(p_lat))*cos(radians(lat))
          * sin(radians(lon-p_lon)/2)^2 ))) as nearest_mi,
      jsonb_agg(jsonb_build_object('time_utc',time_utc,'mag',mag,'city',city,
        'source',source,'measured',measured,
        'dist_mi', round((2*3958.8*asin(sqrt( sin(radians(lat-p_lat)/2)^2
          + cos(radians(p_lat))*cos(radians(lat))
          * sin(radians(lon-p_lon)/2)^2 )))::numeric,1))) as reports
    from hz_lsr
    where kind='hail'
      and lon between p_lon-1.5*dlo and p_lon+1.5*dlo
      and lat between p_lat-1.5*dla and p_lat+1.5*dla
      and date between s and e
      and 2*3958.8*asin(sqrt( sin(radians(lat-p_lat)/2)^2
        + cos(radians(p_lat))*cos(radians(lat))
        * sin(radians(lon-p_lon)/2)^2 )) <= 15
    group by date),
  hdates as (select date from hagg union select date from hrpt),
  hcalc as (
    select d.date,
      case when g.at_mi<=1.0 then g.at_v end as at_v,
      round(g.at_mi::numeric,1) at_mi, g.r1, g.r3, g.r10,
      r.sz3, r.nearest_mi, r.reports,
      -- SAHE-1: base -> adjacent-cell inference -> report floor
      round(greatest(
        coalesce(case when g.at_mi<=1.0 then g.at_v end,
                 0.85*g.r1, 0),
        case when r.sz3 is not null and g.r3 is not null
             then least(r.sz3, g.r3) else 0 end)::numeric, 2) as sahe,
      case when (case when r.sz3 is not null and g.r3 is not null
                      then least(r.sz3, g.r3) else 0 end)
                > coalesce(case when g.at_mi<=1.0 then g.at_v end,
                           0.85*g.r1, 0)
             then 'report_reconciliation'
           when g.at_mi<=1.0 then 'mesh_at_property'
           when g.r1 is not null then 'adjacent_cell_inference'
           else 'ring_only' end as method
    from hdates d
    left join hagg g on g.date=d.date
    left join hrpt r on r.date=d.date)
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'date', date,
      'estimate', jsonb_build_object(
        'max_hail_in', nullif(sahe,0),
        'range_in', jsonb_build_array(
          round(coalesce(at_v, 0.7*r1, 0)::numeric,2),
          round(greatest(coalesce(r1,0), coalesce(sahe,0),
                         coalesce(sz3,0))::numeric,2)),
        'method', 'SAHE-1/'||method,
        'verdict', case
          when nearest_mi <= 1 or coalesce(at_v,0) >= 1.0 then 'confirmed_nearby'
          when coalesce(at_v,0) >= 0.75
               or (r1 is not null and nearest_mi <= 3) then 'likely'
          when r1 is not null or nearest_mi <= 3 then 'possible'
          when r10 is not null then 'in_area_only'
          else 'reported_area' end),
      'inputs', jsonb_build_object(
        'mesh', jsonb_build_object('at_property_in',at_v,'nearest_cell_mi',
            at_mi,'r1_in',r1,'r3_in',r3,'r10_in',r10),
        'storm_reports', coalesce(reports,'[]'::jsonb))
    ) order by date desc), '[]'::jsonb),
    (select jsonb_build_object('inches', nullif(sahe,0), 'date', date,
        'verdict', case
          when nearest_mi <= 1 or coalesce(at_v,0) >= 1.0 then 'confirmed_nearby'
          when coalesce(at_v,0) >= 0.75
               or (r1 is not null and nearest_mi <= 3) then 'likely'
          when r1 is not null or nearest_mi <= 3 then 'possible'
          else 'in_area_only' end)
     from hcalc where sahe > 0 order by sahe desc, date desc limit 1)
  into hail, hsum from hcalc;

  return jsonb_build_object(
    'version','sa-hazard-engine-1.1',
    'generated_utc', now() at time zone 'utc',
    'lat',p_lat,'lon',p_lon,
    'period', jsonb_build_object('start',s,'end',e),
    'summary', jsonb_build_object('max_wind', wsum, 'max_hail', hsum),
    'methodology', jsonb_build_object(
      'wind','SAWE-1: NWS 2.5 km analysis-of-record background; +50% of any '
        ||'upward HRRR (radar-assimilating) disagreement at the property '
        ||'cell; distance-weighted station residual correction (measured '
        ||'minus analysis at stations <=30 mi, weights 1/(1+d/10)^2, clamped '
        ||'±15 mph); result bounded by at-property sources below and by '
        ||'1-mile maxima / measured gusts <=10 mi above. Estimates are '
        ||'inferred, not measured at the property; every input is included.',
      'hail','SAHE-1: MRMS MESH at-property 1 km cell; if absent, 0.85x the '
        ||'1-mile maximum (adjacent-cell inference); ground reports <=3 mi '
        ||'set a floor of min(report size, 3-mile MESH max). Verdict '
        ||'categories reflect report proximity and radar support.',
      'durations','Analysis hours at/above 40 and 58 mph at the property '
        ||'cell (hourly resolution).'),
    'wind_days', wind, 'hail_days', hail);
end $$;


-- ------------------------------------------------------- backfill cursors
create table if not exists hz_backfill (key text primary key, value text);
alter table hz_backfill enable row level security;

create or replace function hz_backfill_get(p_key text) returns text
language sql stable security definer set search_path=public as
$$ select value from hz_backfill where key=p_key $$;

create or replace function hz_backfill_set(p_secret text, p_key text, p_value text)
returns void language plpgsql security definer set search_path=public as $$
begin
  if not sa_secret_ok(p_secret) then raise exception 'forbidden'; end if;
  insert into hz_backfill values (p_key, p_value)
    on conflict (key) do update set value=excluded.value;
end $$;
