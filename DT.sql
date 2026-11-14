----------------------------------------------------------------------
-- 6. DYNAMIC TABLES (declarative marts on top of the star schema)
----------------------------------------------------------------------
-- Level 1: daily trips by user type
CREATE OR REPLACE DYNAMIC TABLE mart.dt_daily_trips
  TARGET_LAG = '5 minutes'
  WAREHOUSE  = practice_wh
AS
SELECT
  d.full_date, d.year, d.month_name, d.day_name, d.is_weekend,
  u.usertype, u.gender_desc,
  COUNT(*)                         AS trips,
  SUM(f.trip_duration_sec) / 60    AS total_minutes,
  AVG(f.trip_duration_sec) / 60    AS avg_minutes,
  AVG(f.rider_age)                 AS avg_rider_age
FROM core.fact_trips f
JOIN core.dim_date d ON d.date_key = f.date_key
JOIN core.dim_user u ON u.user_key = f.user_key
GROUP BY ALL;

-- Level 2a: station usage (intermediate, refreshes only when needed downstream)
CREATE OR REPLACE DYNAMIC TABLE mart.dt_station_usage
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE  = practice_wh
AS
SELECT
  s.station_id, s.latitude, s.longitude,
  d.year, d.month,
  COUNT(*)                       AS trips_started,
  AVG(f.trip_duration_sec) / 60  AS avg_minutes
FROM core.fact_trips f
JOIN core.dim_station s ON s.station_key = f.start_station_key
JOIN core.dim_date d    ON d.date_key    = f.date_key
GROUP BY ALL;

-- Level 3: ranking chained on top of Level 2a
CREATE OR REPLACE DYNAMIC TABLE mart.dt_station_rank
  TARGET_LAG = '10 minutes'
  WAREHOUSE  = practice_wh
AS
SELECT
  year, month, station_id, trips_started,
  RANK() OVER (PARTITION BY year, month ORDER BY trips_started DESC) AS rnk
FROM mart.dt_station_usage;

SHOW DYNAMIC TABLES IN SCHEMA mart;   -- check refresh_mode (INCREMENTAL / FULL)
