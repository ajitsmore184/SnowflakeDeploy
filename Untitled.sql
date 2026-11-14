ALTER TASK CITIBIKE_DB.staging.t_load_staging  SUSPEND;   -- root first
ALTER TASK CITIBIKE_DB.staging.t_load_dim_station SUSPEND;
ALTER TASK CITIBIKE_DB.staging.t_load_dim_user    SUSPEND;
ALTER TASK CITIBIKE_DB.staging.t_load_fact_trips  SUSPEND;
ALTER DYNAMIC TABLE CITIBIKE_DB.mart.dt_daily_trips   SUSPEND;
ALTER DYNAMIC TABLE CITIBIKE_DB.mart.dt_station_usage SUSPEND;
ALTER DYNAMIC TABLE CITIBIKE_DB.mart.dt_station_rank  SUSPEND;


--ALTER TASK CITIBIKE_DB.staging.t_load_staging RESUME;   -- root first
ALTER TASK CITIBIKE_DB.staging.t_load_dim_station RESUME;
ALTER TASK CITIBIKE_DB.staging.t_load_dim_user    RESUME;
ALTER TASK CITIBIKE_DB.staging.t_load_fact_trips  RESUME;
ALTER DYNAMIC TABLE CITIBIKE_DB.mart.dt_daily_trips   RESUME;
ALTER DYNAMIC TABLE CITIBIKE_DB.mart.dt_station_usage RESUME;
ALTER DYNAMIC TABLE CITIBIKE_DB.mart.dt_station_rank  RESUME;

CITIBIKE_DB