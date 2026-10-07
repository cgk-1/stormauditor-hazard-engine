\set ON_ERROR_STOP on
\pset format unaligned
\pset tuples_only on
-- helpers
create temp table t_res(name text, ok bool, got text);
create or replace function pg_temp.chk(n text, cond bool, got text) returns void language sql as $$ insert into t_res values (n, cond, got) $$;
-- 1 bad secret raises
do $$ begin perform hz_p5_roll_v1('nope','HRRR','2024-07-24'); raise exception 'no-raise'; exception when others then if sqlerrm <> 'unauthorized' then raise; end if; end $$;
select pg_temp.chk('bad secret raises', true, 'unauthorized');
-- 2 ANL 07-22: ANL archive starts 07-24 -> refused, nothing written
insert into wind_points(state,valid_date,mph_val,geom) values ('Texas','2024-07-22',55,st_setsrid(st_point(-100.123,30.456),4326));
insert into hz_bg_coarse values ('2024-07-22','ANL',-100.1,30.4,22);
select pg_temp.chk('ANL 07-22 refused (hole 07-23)', r->>'status'='refused' and r->>'error' like 'ANL: archive starts 2024-07-24%', r::text) from (select hz_p5_roll_v1('S3CRET','ANL','2024-07-22','t') r) z;
select pg_temp.chk('ANL raw untouched', (select count(*) from wind_points where valid_date='2024-07-22')=1 and not exists (select 1 from hz_arch_days where date='2024-07-22'), '');
-- 3 HRRR 07-24 ok
insert into hz_hrrr_points values ('Texas','2024-07-24',-100.1,30.1,41,2,0),('Texas','2024-07-24',-100.2,30.1,36,1,0);
insert into hz_bg_coarse values ('2024-07-24','HRRR',-100.1,30.1,12),('2024-07-24','HRRR',-100.2,30.1,9);
select pg_temp.chk('HRRR 07-24 rolled', r->>'ok'='true', r::text) from (select hz_p5_roll_v1('S3CRET','HRRR','2024-07-24','t') r) z;
select 1 where false;
select pg_temp.chk('HRRR 07-24 listed+raw deleted+cursor', (select count(*) from hz_arch_days where date='2024-07-24' and src in ('HRRR','BGH'))=2 and not exists(select 1 from hz_hrrr_points where date='2024-07-24') and not exists(select 1 from hz_bg_coarse where date='2024-07-24') and (select value from hz_backfill where key='p5_hrrr')='2024-07-24', '');
-- 4 HRRR 07-22 skipping 07-23 -> refused by cursor
select pg_temp.chk('HRRR skip refused', r->>'status'='refused' and r->>'error' like 'cursor p5_hrrr%', r::text) from (select hz_p5_roll_v1('S3CRET','HRRR','2024-07-22','t') r) z;
-- 5 HRRR 07-23 with BGH empty -> failed atomically (HRRR not archived)
insert into hz_hrrr_points values ('Texas','2024-07-23',-100.1,30.1,50,3,0);
select pg_temp.chk('HRRR 07-23 BGH empty -> failed', r->>'status'='failed' and r->>'error' like 'BGH 2024-07-23: no raw rows%', r::text) from (select hz_p5_roll_v1('S3CRET','HRRR','2024-07-23','t') r) z;
select pg_temp.chk('HRRR 07-23 atomic: nothing listed, raw kept, cursor same', not exists(select 1 from hz_arch_days where date='2024-07-23' and src='HRRR') and exists(select 1 from hz_hrrr_points where date='2024-07-23') and not exists (select 1 from hz_arch where date='2024-07-23') and (select value from hz_backfill where key='p5_hrrr')='2024-07-24', '');
-- 6 value out of bounds
insert into hz_bg_coarse values ('2024-07-23','HRRR',-100.1,30.1,12);
update hz_hrrr_points set v=34 where date='2024-07-23';
select pg_temp.chk('HRRR v<35 -> failed', r->>'status'='failed' and r->>'error' like '%outside 35-300%', r::text) from (select hz_p5_roll_v1('S3CRET','HRRR','2024-07-23','t') r) z;
update hz_hrrr_points set v=50 where date='2024-07-23';
select pg_temp.chk('HRRR 07-23 ok after fix', r->>'ok'='true', r::text) from (select hz_p5_roll_v1('S3CRET','HRRR','2024-07-23','t') r) z;
-- 7 already archived / repeat -> refused by cursor
select pg_temp.chk('repeat refused', r->>'status'='refused', r::text) from (select hz_p5_roll_v1('S3CRET','HRRR','2024-07-23','t') r) z;
-- 8 HAIL: first day must be ceiling
select pg_temp.chk('HAIL first day must be 09-27', r->>'status'='refused' and r->>'error' like 'cursor p5_hail unset%', r::text) from (select hz_p5_roll_v1('S3CRET','HAIL','2023-09-26','t') r) z;
-- 9 HAIL off-lattice -> failed
insert into hail_points(state,valid_date,in_val,geom,window_end_utc) values ('Texas','2023-09-27',1.25,st_setsrid(st_point(-100.125,30.455),4326),'2023-09-28 05:00Z'),('Texas','2023-09-27',1.00,st_setsrid(st_point(-100.124,30.455),4326),'2023-09-28 05:00Z');
select pg_temp.chk('HAIL off lattice failed', r->>'status'='failed' and r->>'error' like '%off the MRMS%', r::text) from (select hz_p5_roll_v1('S3CRET','HAIL','2023-09-27','t') r) z;
-- duplicate
update hail_points set geom=st_setsrid(st_point(-100.125,30.455),4326) where in_val=1.00;
select pg_temp.chk('HAIL duplicate failed', r->>'status'='failed' and r->>'error' like '%duplicate%', r::text) from (select hz_p5_roll_v1('S3CRET','HAIL','2023-09-27','t') r) z;
update hail_points set geom=st_setsrid(st_point(-100.135,30.455),4326) where in_val=1.00;
select pg_temp.chk('HAIL ok', r->>'ok'='true', r::text) from (select hz_p5_roll_v1('S3CRET','HAIL','2023-09-27','t') r) z;
-- 10 HAIL zero-row day ok
select pg_temp.chk('HAIL empty day ok', r->>'ok'='true', r::text) from (select hz_p5_roll_v1('S3CRET','HAIL','2023-09-26','t') r) z;
select pg_temp.chk('HAIL empty day listed', exists(select 1 from hz_arch_days where src='HAIL' and date='2023-09-26'), '');
-- 11 DB size guard
select pg_temp.chk('size guard', r->>'status'='refused' and r->>'error' like '%GB guard%', r::text) from (select hz_p5_roll_v1('S3CRET','HAIL','2023-09-25','t',0.001) r) z;
-- 12 OBS
insert into hz_station_daily values ('DEN','2024-07-21',40);
insert into hz_station_peak(stid,date,peak_mph) values ('DEN','2024-07-21',41);
select pg_temp.chk('OBS missing v4 failed', r->>'status'='failed' and r->>'error' like '%hz_station_daily_v4%', r::text) from (select hz_p5_roll_v1('S3CRET','OBS','2024-07-21','t') r) z;
insert into hz_station_daily_v4(stid,date,gust_mph) values ('DEN','2024-07-21',41);
select pg_temp.chk('OBS ok', r->>'ok'='true', r::text) from (select hz_p5_roll_v1('S3CRET','OBS','2024-07-21','t') r) z;
select pg_temp.chk('OBS cursor', (select value from hz_backfill where key='p5_obs')='2024-07-21', '');
-- 13 NCEI
insert into hz_storm_events select 'e'||g, date '2023-01-01' + (g % 365) from generate_series(1,1500) g;
insert into hz_storm_events_v4 select 'e'||g, 2023 from generate_series(1,1499) g;
select pg_temp.chk('NCEI v4 count mismatch failed', r->>'status'='failed', r::text) from (select hz_p5_roll_v1('S3CRET','NCEI','2023-01-01','t') r) z;
insert into hz_storm_events_v4 values ('e1500',2023);
select pg_temp.chk('NCEI 2023 ok', r->>'ok'='true', r::text) from (select hz_p5_roll_v1('S3CRET','NCEI','2023-01-01','t') r) z;
select pg_temp.chk('NCEI cursor', (select value from hz_backfill where key='p5_ncei')='2023-01-01', '');
select pg_temp.chk('NCEI 2021 before 2022 refused', r->>'status'='refused', r::text) from (select hz_p5_roll_v1('S3CRET','NCEI','2021-01-01','t') r) z;
select pg_temp.chk('NCEI mid-year refused', r->>'status'='refused', r::text) from (select hz_p5_roll_v1('S3CRET','NCEI','2022-03-01','t') r) z;
-- 14 window: day above ceiling / below floor
select pg_temp.chk('ANL 07-23 refused (ceiling)', r->>'status'='refused' and r->>'error' like '%outside the lane window%', r::text) from (select hz_p5_roll_v1('S3CRET','ANL','2024-07-23','t') r) z;
select pg_temp.chk('below floor refused', r->>'status'='refused', r::text) from (select hz_p5_roll_v1('S3CRET','OBS','2021-09-30','t') r) z;
-- 15 ANL once 07-23 is listed (owner option): ANL+BGA roll together
insert into hz_arch_days(src,date) values ('ANL','2024-07-23');
select pg_temp.chk('ANL 07-22 ok after 07-23 listed', r->>'ok'='true', r::text) from (select hz_p5_roll_v1('S3CRET','ANL','2024-07-22','t') r) z;
select pg_temp.chk('ANL+BGA 07-22 listed, raw gone', (select count(*) from hz_arch_days where date='2024-07-22' and src in ('ANL','BGA'))=2 and not exists(select 1 from wind_points where valid_date='2024-07-22') and not exists(select 1 from hz_bg_coarse where src='ANL' and date='2024-07-22'), '');
-- 16 status fn
select pg_temp.chk('status', (s->'arch_min'->>'HRRR')='2024-07-23' and (s->'arch_min'->>'BGH')='2024-07-23' and (s->'cursors'->>'p5_anl')='2024-07-22', s::text) from (select hz_p5_status_v1('S3CRET') s) z;
-- 17 privileges
select pg_temp.chk('anon cannot roll', not has_function_privilege('anon','hz_p5_roll_v1(text,text,date,text,numeric)','execute'), '');
select pg_temp.chk('authenticated cannot roll', not has_function_privilege('authenticated','hz_p5_roll_v1(text,text,date,text,numeric)','execute'), '');
select pg_temp.chk('service_role can roll', has_function_privilege('service_role','hz_p5_roll_v1(text,text,date,text,numeric)','execute'), '');
select pg_temp.chk('anon can status', has_function_privilege('anon','hz_p5_status_v1(text)','execute'), '');
select pg_temp.chk('anon no log table', not has_table_privilege('anon','hz_backfill_log','select'), '');
-- 18 median warn: 12 fake rolled HRRR log rows median 1000, then a day with 2 rows -> rolled_warn
insert into hz_backfill_log(lane,src,day,status,rows_raw) select 'HRRR','HRRR','2020-01-01','rolled',1000 from generate_series(1,12);
insert into hz_backfill_log(lane,src,day,status,rows_raw) select 'HRRR','BGH','2020-01-01','rolled',2 from generate_series(1,12);
insert into hz_hrrr_points values ('Texas','2024-07-22',-100.1,30.1,41,2,0),('Texas','2024-07-22',-100.2,30.1,36,1,0);
insert into hz_bg_coarse values ('2024-07-22','HRRR',-100.1,30.1,12),('2024-07-22','HRRR',-100.2,30.1,9);
select pg_temp.chk('median warn', r->>'ok'='true' and r->>'status'='rolled_warn', r::text) from (select hz_p5_roll_v1('S3CRET','HRRR','2024-07-22','t') r) z;
insert into hz_hrrr_points select 'Texas','2024-07-21',-100+g*0.001,30.1,41,2,0 from generate_series(1,20001) g;
insert into hz_bg_coarse values ('2024-07-21','HRRR',-100.1,30.1,12),('2024-07-21','HRRR',-100.2,30.1,9);
select pg_temp.chk('median >10x refused', r->>'status'='failed' and r->>'error' like '%10x%', r::text) from (select hz_p5_roll_v1('S3CRET','HRRR','2024-07-21','t') r) z;
select (case when ok then 'PASS ' else 'FAIL ' end)||name||case when ok then '' else ' :: '||got end from t_res;
select count(*) filter (where ok)||'/'||count(*) from t_res;
select status, count(*) from hz_backfill_log group by 1 order by 1;
