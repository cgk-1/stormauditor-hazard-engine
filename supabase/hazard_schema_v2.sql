-- ============================================================================
-- StormAuditor HAZARD ENGINE v2 — schema (Lovable migration)
--
-- DESIGN: your existing Wind Explorer and Hail Explorer tables ARE the
-- analysis-of-record wind layer and the MESH hail layer (2 years, local-clock
-- state days). This engine adds only what they lack:
--   hz_hrrr_points     independent HRRR 3-km daily-max layer (local days)
--   hz_station_bg      daily-max background sampled at every station
--   hz_stations/_daily measured ASOS/AWOS peak gusts        (already loaded)
--   hz_lsr             NWS Local Storm Reports w/ measured flag (already)
--   hz_storm_events    finalized NCEI record                 (already loaded)
--   hz_hurdat          NHC best track                        (already loaded)
-- and the estimator: property_hazard_report() running SAWE-2 (successive-
-- correction objective analysis; Cressman 1959, Barnes 1964, Koch et al.
-- 1983) and SAHE-2 (MESH-primary categorical hail; Witt 1998, Wilson 2009,
-- Ortega 2018, Murillo & Homeyer 2019). Estimated storm reports are never
-- used as magnitudes (Edwards et al. 2018). Full citations: RESEARCH_BASIS.md
-- ============================================================================

-- ---------------- shared secret (idempotent; keep existing value)
create table if not exists sa_config (key text primary key, value text not null);
alter table sa_config enable row level security;
create or replace function sa_secret_ok(p_secret text) returns boolean
language sql stable security definer set search_path = public as $$
  select exists(select 1 from sa_config
                where key='ingest_secret' and value=p_secret);
$$;

-- ============================================================
-- ADAPTER VIEWS — the ONLY part Lovable must adapt.
-- Point them at your real Explorer tables/columns:
--   hz_v_anl : the Wind Explorer per-cell daily-max points
--              (the table written by ingest_wind_points)
--   hz_v_hail: the Hail Explorer per-cell MESH points
--              (the table written by ingest_points)
-- ============================================================
create or replace view hz_v_anl as
  select date, lon::real as lon, lat::real as lat, v::smallint as v
  from wind_points;            -- <-- ADAPT table/column names

create or replace view hz_v_hail as
  select date, lon::real as lon, lat::real as lat, v::real as v
  from hail_points;            -- <-- ADAPT table/column names

-- ---------------- observation tables (idempotent: created earlier)
create table if not exists hz_stations (
  stid text primary key, name text, state text,
  lat real, lon real, network text);
create index if not exists hz_stations_geo_idx on hz_stations (lon, lat);

create table if not exists hz_station_daily (
  stid text, date date, gust_mph real, primary key (stid, date));
create index if not exists hz_station_daily_date_idx on hz_station_daily (date);

create table if not exists hz_lsr (
  id bigint generated always as identity primary key,
  time_utc text, date date, kind text check (kind in ('wind','hail')),
  type text, mag text, lat real, lon real,
  city text, state text, source text, measured boolean);
create index if not exists hz_lsr_geo_idx on hz_lsr (kind, lon, lat, date);

create table if not exists hz_storm_events (
  event_id text primary key, kind text, event_type text,
  begin_utc text, date date, lat real, lon real,
  magnitude text, mag_type text, state text, cz_name text);
create index if not exists hz_se_geo_idx on hz_storm_events (kind, lon, lat, date);

create table if not exists hz_hurdat (
  storm_id text, name text, time_utc text, date date,
  status text, lat real, lon real, wind_kt int,
  primary key (storm_id, time_utc));
create index if not exists hz_hurdat_date_idx on hz_hurdat (date);

-- ---------------- NEW: HRRR layer (local-clock state days)
create table if not exists hz_hrrr_points (
  state text not null,
  date  date not null,
  lon   real not null,
  lat   real not null,
  v     smallint not null,
  d40   smallint not null default 0,
  d58   smallint not null default 0,
  primary key (state, date, lon, lat)
);
create index if not exists hz_hrrr_geo_idx on hz_hrrr_points (lon, lat, date);

create table if not exists hz_hrrr_meta (
  state text, date date, hours int, primary key (state, date));

-- ---------------- NEW: background-at-station (both sources)
create table if not exists hz_station_bg (
  date date not null,
  src  text not null check (src in ('ANL','HRRR')),
  stid text not null,
  bg   smallint not null,
  primary key (date, src, stid)
);

-- ---------------- backfill cursors (idempotent)
create table if not exists hz_backfill (key text primary key, value text);
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

-- ---------------- RLS: no client policies; reads via the report function
alter table hz_hrrr_points   enable row level security;
alter table hz_hrrr_meta     enable row level security;
alter table hz_station_bg    enable row level security;
alter table hz_stations      enable row level security;
alter table hz_station_daily enable row level security;
alter table hz_lsr           enable row level security;
alter table hz_storm_events  enable row level security;
alter table hz_hurdat        enable row level security;
alter table hz_backfill      enable row level security;

-- ---------------- ingest RPCs (new + kept)
create or replace function hz_hrrr_ingest(p_secret text, p_state text,
  p_date date, p_hours int, p_points jsonb, p_append boolean)
returns void language plpgsql security definer set search_path=public as $$
begin
  if not sa_secret_ok(p_secret) then raise exception 'forbidden'; end if;
  if not p_append then
    delete from hz_hrrr_points where state=p_state and date=p_date;
    insert into hz_hrrr_meta values (p_state, p_date, p_hours)
      on conflict (state,date) do update set hours=excluded.hours;
  end if;
  insert into hz_hrrr_points
  select p_state, p_date, (p->>'lon')::real, (p->>'lat')::real,
         (p->>'v')::smallint, (p->>'d40')::smallint, (p->>'d58')::smallint
  from jsonb_array_elements(p_points) p
  on conflict do nothing;
end $$;

create or replace function hz_station_bg_ingest(p_secret text, p_date date,
  p_src text, p_rows jsonb, p_append boolean)
returns void language plpgsql security definer set search_path=public as $$
begin
  if not sa_secret_ok(p_secret) then raise exception 'forbidden'; end if;
  if not p_append then
    delete from hz_station_bg where date=p_date and src=p_src;
  end if;
  insert into hz_station_bg
  select p_date, p_src, r->>'stid', (r->>'bg')::smallint
  from jsonb_array_elements(p_rows) r
  on conflict (date,src,stid) do update set bg=excluded.bg;
end $$;

create or replace function hz_stations_fetch(p_secret text)
returns jsonb language sql stable security definer set search_path=public as $$
  select coalesce(jsonb_agg(jsonb_build_object(
    'stid',stid,'lat',lat,'lon',lon,'state',state)),'[]'::jsonb)
  from hz_stations
  where sa_secret_ok(p_secret);
$$;

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
  if not p_append then delete from hz_hurdat where true; end if;
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
  delete from hz_hrrr_points   where date < current_date - 730;
  delete from hz_station_bg    where date < current_date - 730;
  delete from hz_station_daily where date < current_date - 730;
  delete from hz_lsr           where date < current_date - 730;
  delete from hz_storm_events  where date < current_date - 730;
end $$;

-- ============================================================================
-- property_hazard_report v2.1 — SAWE-2 / SAHE-2 over Explorer data + HRRR
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
  wind jsonb; hail jsonb; wsum_j jsonb; hsum_j jsonb;
begin
  -- ---------------- wind cells within 10 mi: ANL from the Explorer view,
  --                  HRRR from the new layer
  create temp table _w on commit drop as
    select date, src, v, d40, d58,
           2*3958.8*asin(sqrt( sin(radians(lat-p_lat)/2)^2
             + cos(radians(p_lat))*cos(radians(lat))
             * sin(radians(lon-p_lon)/2)^2 )) as mi
    from (
      select date, 'ANL'::text src, lon, lat, v,
             null::smallint d40, null::smallint d58
      from hz_v_anl
      where lon between p_lon-dlo and p_lon+dlo
        and lat between p_lat-dla and p_lat+dla
        and date between s and e
      union all
      select date, 'HRRR', lon, lat, v, d40, d58
      from hz_hrrr_points
      where lon between p_lon-dlo and p_lon+dlo
        and lat between p_lat-dla and p_lat+dla
        and date between s and e
    ) u;
  delete from _w where mi > 10.0;

  -- ---------------- measured observations within 50 mi, with exact
  --                  backgrounds (hz_station_bg) or Explorer-cell fallback
  create temp table _obs on commit drop as
    select d.date, d.gust_mph::numeric as obs, st.stid, st.name,
      (2*3958.8*asin(sqrt( sin(radians(st.lat-p_lat)/2)^2
        + cos(radians(p_lat))*cos(radians(st.lat))
        * sin(radians(st.lon-p_lon)/2)^2 )))::numeric as mi,
      b.bg as bg
    from hz_station_daily d
    join hz_stations st using (stid)
    left join lateral (
      select coalesce(
        (select bg::numeric from hz_station_bg
          where date=d.date and stid=d.stid and src='ANL'),
        (select w.v::numeric from hz_v_anl w
          where w.date=d.date
            and w.lon between st.lon-0.055 and st.lon+0.055
            and w.lat between st.lat-0.045 and st.lat+0.045
          order by (w.lon-st.lon)^2+(w.lat-st.lat)^2 limit 1)) as bg
    ) b on true
    where st.lon between p_lon-5*dlo and p_lon+5*dlo
      and st.lat between p_lat-5*dla and p_lat+5*dla
      and d.date between s and e
      and d.gust_mph >= 25
      and 2*3958.8*asin(sqrt( sin(radians(st.lat-p_lat)/2)^2
        + cos(radians(p_lat))*cos(radians(st.lat))
        * sin(radians(st.lon-p_lon)/2)^2 )) <= 50;
  insert into _obs
    select l.date, nullif(regexp_replace(l.mag,'[^0-9.]','','g'),'')::numeric,
      'LSR-M', l.city||' ('||l.source||')',
      (2*3958.8*asin(sqrt( sin(radians(l.lat-p_lat)/2)^2
        + cos(radians(p_lat))*cos(radians(l.lat))
        * sin(radians(l.lon-p_lon)/2)^2 )))::numeric,
      (select coalesce(
         (select b2.bg::numeric from hz_station_bg b2
           where b2.date=l.date and b2.src='ANL' and b2.stid=st2.stid),
         (select w.v::numeric from hz_v_anl w
           where w.date=l.date
             and w.lon between l.lon-0.055 and l.lon+0.055
             and w.lat between l.lat-0.045 and l.lat+0.045
           order by (w.lon-l.lon)^2+(w.lat-l.lat)^2 limit 1))
       from hz_stations st2
       order by (st2.lat-l.lat)^2+(st2.lon-l.lon)^2 limit 1)
    from hz_lsr l
    where l.kind='wind' and l.measured and l.mag ~ '[0-9]'
      and l.lon between p_lon-5*dlo and p_lon+5*dlo
      and l.lat between p_lat-5*dla and p_lat+5*dla
      and l.date between s and e
      and 2*3958.8*asin(sqrt( sin(radians(l.lat-p_lat)/2)^2
        + cos(radians(p_lat))*cos(radians(l.lat))
        * sin(radians(l.lon-p_lon)/2)^2 )) <= 50;
  delete from _obs where bg is null or obs is null;

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
  trop as (
    select distinct d from (
      select date d from hz_hurdat where 2*3958.8*asin(sqrt(
        sin(radians(lat-p_lat)/2)^2 + cos(radians(p_lat))*cos(radians(lat))
        * sin(radians(lon-p_lon)/2)^2 )) <= 250
      union all select date-1 from hz_hurdat where 2*3958.8*asin(sqrt(
        sin(radians(lat-p_lat)/2)^2 + cos(radians(p_lat))*cos(radians(lat))
        * sin(radians(lon-p_lon)/2)^2 )) <= 250
      union all select date+1 from hz_hurdat where 2*3958.8*asin(sqrt(
        sin(radians(lat-p_lat)/2)^2 + cos(radians(p_lat))*cos(radians(lat))
        * sin(radians(lon-p_lon)/2)^2 )) <= 250) x
    where d between s and e),
  beststn as (
    select distinct on (date) date, obs gust_mph, stid, name,
      round(mi,1) mi
    from _obs where stid <> 'LSR-M' order by date, obs desc),
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
  wdates as (
    select date from layer
    union select date from beststn where gust_mph >= 40
    union select date from rpt where kind='wind'),
  daytype as (
    select w.date,
      case when w.date in (select d from trop) then 'tropical'
           when exists (select 1 from rpt r where r.date=w.date and r.kind='wind')
             or exists (select 1 from layer h where h.date=w.date
                        and h.src='HRRR'
                        and coalesce(h.r10,0) - coalesce(h.at_v, h.r10, 0) >= 15)
           then 'convective' else 'synoptic' end as etype
    from wdates w),
  oa as (
    select o.date,
      sum( exp(-power(o.mi*1.609344,2) /
               (2*power(case dt.etype when 'convective' then 10.0
                        else 50.0 end,2))) * (o.obs - o.bg) ) as num,
      sum( exp(-power(o.mi*1.609344,2) /
               (2*power(case dt.etype when 'convective' then 10.0
                        else 50.0 end,2))) )                  as den,
      max(o.obs - o.bg)                                       as max_innov,
      count(*)                                                as n_obs
    from _obs o join daytype dt using (date)
    group by o.date),
  calc as (
    select w.date, dt.etype,
      a.at_v anl_at, a.at_mi anl_mi, a.r1 anl_r1, a.r3 anl_r3, a.r10 anl_r10,
      h.at_v hr_at, h.r1 hr_r1, h.r3 hr_r3, h.r10 hr_r10,
      h.d40 hr_d40, h.d58 hr_d58,
      st.gust_mph stn_mph, st.stid, st.name stn_name, st.mi stn_mi,
      rw.reports wind_reports, coalesce(rw.any_measured,false) lsr_m,
      sw.events wind_events, coalesce(sw.any_measured,false) se_m,
      (dt.etype='tropical') tropical,
      coalesce(a.at_v, h.at_v)::numeric as bkg,
      greatest(0, least(coalesce(oa.max_innov, 0),
        coalesce(oa.num,0) / (1.0 + coalesce(oa.den,0)))) as corr,
      coalesce(oa.n_obs,0) n_obs, coalesce(oa.den,0) w_den
    from wdates w
    join daytype dt on dt.date=w.date
    left join layer a on a.date=w.date and a.src='ANL'
    left join layer h on h.date=w.date and h.src='HRRR'
    left join oa on oa.date=w.date
    left join beststn st on st.date=w.date
    left join rpt rw on rw.date=w.date and rw.kind='wind'
    left join sev sw on sw.date=w.date and sw.kind='wind'),
  est as (
    select *, round(bkg + corr)::int as sawe
    from calc where bkg is not null)
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'date', date,
      'estimate', jsonb_build_object(
        'max_gust_mph', sawe,
        'range_mph', jsonb_build_array(
          least(coalesce(anl_at,hr_at), coalesce(hr_at,anl_at), sawe),
          greatest(sawe, coalesce(anl_r1,0), coalesce(hr_r1,0),
            case when stn_mi<=10 then coalesce(stn_mph,0)::int else 0 end)),
        'grade', case
          when stn_mph is not null and stn_mi<=10
               and abs(stn_mph - sawe) <= 0.2*greatest(stn_mph, sawe) then 'A'
          when tropical and stn_mph is not null then 'A'
          when (stn_mph is not null and stn_mi<=25) or lsr_m or se_m then 'B'
          else 'C' end,
        'method', 'SAWE-2 (objective analysis)',
        'event_type', etype,
        'background_mph', bkg,
        'oa_correction_mph', round(corr,1),
        'oa_observations', n_obs,
        'oa_weight_sum', round(w_den,3)),
      'hrs_ge_40', hr_d40, 'hrs_ge_58', hr_d58,
      'duration_source', case when hr_d40 is not null then 'HRRR' end,
      'tropical', tropical,
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
        'event_type', etype,
        'range_mph', jsonb_build_array(
          least(coalesce(anl_at,hr_at), coalesce(hr_at,anl_at), sawe),
          greatest(sawe, coalesce(anl_r1,0), coalesce(hr_r1,0),
            case when stn_mi<=10 then coalesce(stn_mph,0)::int else 0 end)),
        'grade', case
          when stn_mph is not null and stn_mi<=10
               and abs(stn_mph - sawe) <= 0.2*greatest(stn_mph, sawe) then 'A'
          when tropical and stn_mph is not null then 'A'
          when (stn_mph is not null and stn_mi<=25) or lsr_m or se_m then 'B'
          else 'C' end)
     from est order by sawe desc, date desc limit 1)
  into wind, wsum_j from est;

  -- ---------------- hail: SAHE-2 over the Explorer MESH view
  with hp as (
    select date, v,
      2*3958.8*asin(sqrt( sin(radians(lat-p_lat)/2)^2
        + cos(radians(p_lat))*cos(radians(lat))
        * sin(radians(lon-p_lon)/2)^2 )) as mi
    from hz_v_hail
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
      round(greatest(
        coalesce(case when g.at_mi<=1.0 then g.at_v end, g.r1, 0),
        case when r.sz3 is not null and g.r3 is not null
             then least(r.sz3, g.r3) else 0 end)::numeric, 2) as sahe,
      case when (case when r.sz3 is not null and g.r3 is not null
                      then least(r.sz3, g.r3) else 0 end)
                > coalesce(case when g.at_mi<=1.0 then g.at_v end, g.r1, 0)
             then 'report_reconciliation'
           when g.at_mi<=1.0 then 'mesh_at_property'
           when g.r1 is not null then 'mesh_within_1mi'
           else 'reports_only' end as method
    from hdates d
    left join hagg g on g.date=d.date
    left join hrpt r on r.date=d.date)
  select
    coalesce(jsonb_agg(jsonb_build_object(
      'date', date,
      'estimate', jsonb_build_object(
        'max_hail_in', nullif(sahe,0),
        'size_category', case
          when sahe >= 2.0  then 'significant_severe (>=2.0 in)'
          when sahe >= 1.0  then 'severe (1.0-1.99 in)'
          when sahe >= 0.5  then 'sub_severe (0.50-0.99 in)'
          when sahe > 0     then 'small (<0.50 in)'
          else null end,
        'range_in', jsonb_build_array(
          round(coalesce(at_v, 0)::numeric,2),
          round(greatest(coalesce(r1,0), coalesce(sahe,0),
                         coalesce(sz3,0))::numeric,2)),
        'method', 'SAHE-2/'||method,
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
  into hail, hsum_j from hcalc;

  return jsonb_build_object(
    'version','sa-hazard-engine-2.1',
    'generated_utc', now() at time zone 'utc',
    'lat',p_lat,'lon',p_lon,
    'period', jsonb_build_object('start',s,'end',e),
    'summary', jsonb_build_object('max_wind', wsum_j, 'max_hail', hsum_j),
    'day_convention','Local calendar day of the state''s dominant timezone '
      ||'(DST-aware), matching the Explorer pipelines. Tropical flagging '
      ||'uses a +/-1-day best-track window.',
    'methodology', jsonb_build_object(
      'wind','SAWE-2: successive-correction objective analysis (Cressman '
        ||'1959; Barnes 1964; Koch et al. 1983). Background = NWS 2.5 km '
        ||'analysis-of-record daily max gust at the property cell (HRRR '
        ||'where analysis absent). Correction = background-weighted '
        ||'Barnes-Gaussian mean of measured-observation innovations '
        ||'(measured ASOS/AWOS peaks; measured-qualifier storm reports; '
        ||'estimated reports excluded per Edwards et al. 2018). Length '
        ||'scale 10 km convective / 50 km tropical-synoptic (WMO TD-1555). '
        ||'One-sided (documented analysis low bias in strong wind), capped '
        ||'at the largest observed innovation.',
      'hail','SAHE-2: MRMS MESH primary (Witt et al. 1998); categorical '
        ||'size reporting (Wilson et al. 2009; Ortega 2018; Murillo & '
        ||'Homeyer 2019); ground reports reconcile occurrence, size floored '
        ||'at min(report, 3-mi MESH max); MESH swaths preferred to reports '
        ||'for detection (Melick et al. 2014).',
      'durations','HRRR hours at/above 40 and 58 mph at the property cell '
        ||'(hourly resolution).'),
    'wind_days', wind, 'hail_days', hail);
end $$;
