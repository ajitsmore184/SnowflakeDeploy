----------------------------------------------------------------------
-- 8. MONITORING
----------------------------------------------------------------------
-- Task runs
SELECT name, state, scheduled_time, completed_time, error_message
FROM TABLE(citibike_db.INFORMATION_SCHEMA.TASK_HISTORY(
       SCHEDULED_TIME_RANGE_START => DATEADD('hour', -2, CURRENT_TIMESTAMP())))
ORDER BY scheduled_time DESC;

-- Dynamic table refreshes
SELECT name, state, refresh_action, data_timestamp, refresh_trigger
FROM TABLE(citibike_db.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
ORDER BY data_timestamp DESC
LIMIT 20;

-- Row counts across layers
SELECT 'raw'      AS layer, COUNT(*) FROM raw.trips_raw
UNION ALL SELECT 'staging', COUNT(*) FROM staging.trips_clean
UNION ALL SELECT 'fact',    COUNT(*) FROM core.fact_trips
UNION ALL SELECT 'dim_station', COUNT(*) FROM core.dim_station
UNION ALL SELECT 'dim_user',    COUNT(*) FROM core.dim_user;
