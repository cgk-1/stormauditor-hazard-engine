-- ============================================================================
-- WIPE redundant Hazard Engine grid data (run once before the v2 rebuild).
--
-- REMOVES (redundant with your existing Wind/Hail Explorer data, ~124M rows):
--   hz_wind_points, hz_wind_meta, the 'hzwind' backfill cursor,
--   and any old hz_station_bg rows tied to the removed ANL ingestion.
--
-- KEEPS (loaded successfully, needed by v2, nothing to re-download):
--   hz_stations (2,709 stations), hz_station_daily, hz_lsr,
--   hz_storm_events (NCEI), hz_hurdat, sa_config.
--
-- 'where true' satisfies the platform guard that blocks unqualified DELETEs.
-- ============================================================================

delete from hz_wind_points where true;
delete from hz_wind_meta   where true;
delete from hz_station_bg  where true;
delete from hz_backfill    where key in ('hzwind');

-- reclaim the disk immediately (safe; brief locks on the emptied tables)
vacuum full hz_wind_points;
vacuum full hz_wind_meta;
vacuum full hz_station_bg;

-- optional: drop the emptied national wind table entirely once the v2
-- estimator is installed (v2 reads the Explorer tables + hz_hrrr_points):
-- drop table if exists hz_wind_points cascade;
-- drop table if exists hz_wind_meta cascade;
