-- Stage 5 local test, part 1 (mock extras). Run on PG16 + PostGIS after stormwatch-insight
-- scripts/phase5-stage4/mock_schema.sql, then the migration, then test_roll.sql (expects 38/38 PASS).
\set ON_ERROR_STOP on
create table hz_backfill(key text primary key, value text);
create table hz_storm_events(event_id text primary key, date date);
create table hz_storm_events_v4(event_id text primary key, ncei_year int);
alter table hz_arch_days add primary key (src, date);
create or replace function hz_arch_verify_day(p_src text, p_date date) returns jsonb language plpgsql stable as $$
declare n_live bigint; n_arch bigint;
begin
  if p_src='HAIL' then select count(*) into n_live from hail_points where valid_date=p_date;
  elsif p_src='ANL' then select count(*) into n_live from wind_points where valid_date=p_date;
  elsif p_src='HRRR' then select count(*) into n_live from hz_hrrr_points where date=p_date;
  else select count(*) into n_live from hz_bg_coarse where src=case p_src when 'BGA' then 'ANL' else 'HRRR' end and date=p_date; end if;
  if p_src in ('HAIL','ANL') then select coalesce(sum(cardinality(x)),0) into n_arch from hz_arch_pt where src=p_src and date=p_date;
  else select coalesce(sum(cardinality(lon)),0) into n_arch from hz_arch where src=p_src and date=p_date; end if;
  return jsonb_build_object('live',n_live,'arch',n_arch,'exact',n_live=n_arch);
end $$;
create function hz_arch_pt_pack(p_src text, d0 date, d1 date) returns void language plpgsql as $$ begin
  if p_src='HAIL' then insert into hz_arch_pt(src,tx,ty,date,x,y,v,st) select 'HAIL',0,0,d0, array_agg(round(st_x(geom)*1000)::int), array_agg(round(st_y(geom)*1000)::int), array_agg((in_val*100)::int2), array_agg(1::int2) from hail_points where valid_date=d0 having count(*)>0;
  else insert into hz_arch_pt(src,tx,ty,date,x,y,v,st) select 'ANL',0,0,d0, array_agg(round(st_x(geom)*1000)::int), array_agg(round(st_y(geom)*1000)::int), array_agg(mph_val::int2), array_agg(1::int2) from wind_points where valid_date=d0 having count(*)>0; end if; end $$;
create function hz_arch_grid_pack(p_src text, d0 date, d1 date) returns void language plpgsql as $$ begin
  if p_src='HRRR' then insert into hz_arch(src,tx,ty,date,lon,lat,v) select 'HRRR',0,0,d0, array_agg(lon), array_agg(lat), array_agg(v::real) from hz_hrrr_points where date=d0 having count(*)>0;
  else insert into hz_arch(src,tx,ty,date,lon,lat,v) select p_src,0,0,d0, array_agg(lon), array_agg(lat), array_agg(v::real) from hz_bg_coarse where src=case p_src when 'BGA' then 'ANL' else 'HRRR' end and date=d0 having count(*)>0; end if; end $$;
-- real prod body of hz_arch_roll (2026-10-07)
CREATE OR REPLACE FUNCTION public.hz_arch_roll(p_src text, p_date date, p_delete_live boolean DEFAULT false)
 RETURNS jsonb LANGUAGE plpgsql SET search_path TO 'public' AS $function$
declare v jsonb;
begin
  if exists (select 1 from hz_arch_days where src = p_src and date = p_date) then
    return jsonb_build_object('src',p_src,'date',p_date,'status','already_archived');
  end if;
  if p_src in ('ANL','HAIL') then
    delete from hz_arch_pt where src = p_src and date = p_date;
    perform hz_arch_pt_pack(p_src, p_date, p_date);
  else
    delete from hz_arch where src = p_src and date = p_date;
    perform hz_arch_grid_pack(p_src, p_date, p_date);
  end if;
  v := hz_arch_verify_day(p_src, p_date);
  if not (v->>'exact')::boolean then
    raise exception 'archive roll % % NOT exact, rolled back: %', p_src, p_date, v;
  end if;
  insert into hz_arch_days(src, date) values (p_src, p_date);
  if p_delete_live then
    if    p_src = 'HAIL' then delete from hail_points    where valid_date = p_date;
    elsif p_src = 'ANL'  then delete from wind_points    where valid_date = p_date;
    elsif p_src = 'HRRR' then delete from hz_hrrr_points where date = p_date;
    else  delete from hz_bg_coarse where src = case p_src when 'BGA' then 'ANL' else 'HRRR' end and date = p_date;
    end if;
  end if;
  return jsonb_build_object('src',p_src,'date',p_date,'status','archived','deleted_live',p_delete_live) || v;
end $function$;
-- existing archive starts (prod 2026-10-07)
insert into hz_arch_days(src,date) values ('HAIL','2023-09-28'),('ANL','2024-07-24'),('BGA','2024-07-23'),('HRRR','2024-07-25'),('BGH','2024-07-25');
