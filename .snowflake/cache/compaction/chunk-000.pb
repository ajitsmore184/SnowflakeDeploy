
J
	README.md## the snowflake
*
≥ç·◊ d9d206e37959de0b3b25ff92774917a4
Ò
Raw_layer.sql≥----------------------------------------------------------------------
-- 2. RAW LAYER + STREAM
----------------------------------------------------------------------
CREATE OR REPLACE TABLE raw.trips_raw (
  tripduration            INT,
  starttime               TIMESTAMP_NTZ,
  stoptime                TIMESTAMP_NTZ,
  start_station_id        INT,
  start_station_latitude  FLOAT,
  start_station_longitude FLOAT,
  end_station_id          INT,
  end_station_latitude    FLOAT,
  end_station_longitude   FLOAT,
  bikeid                  INT,
  membership_type         STRING,
  usertype                STRING,
  birth_year              INT,
  gender                  INT,
  src_file                STRING,
  src_row                 NUMBER,
  load_ts                 TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Append-only stream: captures only new rows inserted by COPY
CREATE OR REPLACE STREAM raw.trips_raw_stream
  ON TABLE raw.trips_raw
  APPEND_ONLY = TRUE;

SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18
FROM @raw.ext_citibike_stage/2020/data_0_0_0.csv.gz (FILE_FORMAT => 'raw.ff_csv')
LIMIT 5;
----------------------------------------------------------------------
-- 3. STAGING LAYER (cleansed, deduped batch from stream)
----------------------------------------------------------------------
CREATE OR REPLACE TABLE staging.trips_clean (
  trip_id                 STRING,
  tripduration            INT,
  starttime               TIMESTAMP_NTZ,
  stoptime                TIMESTAMP_NTZ,
  start_station_id        INT,
  start_station_latitude  FLOAT,
  start_station_longitude FLOAT,
  end_station_id          INT,
  end_station_latitude    FLOAT,
  end_station_longitude   FLOAT,
  bikeid                  INT,
  membership_type         STRING,
  usertype                STRING,
  birth_year              INT,
  gender                  INT,
  load_ts                 TIMESTAMP_LTZ
);


=================================

----------------------------------------------------------------------
-- 4. STAR SCHEMA
----------------------------------------------------------------------
CREATE OR REPLACE TABLE core.dim_date (
  date_key     INT PRIMARY KEY,        -- YYYYMMDD
  full_date    DATE,
  year         INT,
  quarter      INT,
  month        INT,
  month_name   STRING,
  day          INT,
  day_of_week  INT,
  day_name     STRING,
  is_weekend   BOOLEAN
);

-- Static dimension: generate ~11 years of dates
INSERT INTO core.dim_date
SELECT
  TO_NUMBER(TO_CHAR(d,'YYYYMMDD')),
  d,
  YEAR(d), QUARTER(d), MONTH(d), MONTHNAME(d), DAY(d),
  DAYOFWEEKISO(d), DAYNAME(d),
  DAYOFWEEKISO(d) IN (6,7)
FROM (
  SELECT DATEADD(day, SEQ4(), '2013-01-01'::DATE) AS d
  FROM TABLE(GENERATOR(ROWCOUNT => 4000))
);

CREATE OR REPLACE TABLE core.dim_station (
  station_key  INT AUTOINCREMENT PRIMARY KEY,
  station_id   INT,
  latitude     FLOAT,
  longitude    FLOAT,
  created_at   TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  updated_at   TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE core.dim_user (
  user_key         INT AUTOINCREMENT PRIMARY KEY,
  membership_type  STRING,
  usertype         STRING,
  gender           INT,
  gender_desc      STRING
);


CREATE OR REPLACE TABLE core.fact_trips (
  trip_id             STRING PRIMARY KEY,
  date_key            INT,
  start_station_key   INT,
  end_station_key     INT,
  user_key            INT,
  start_ts            TIMESTAMP_NTZ,
  stop_ts             TIMESTAMP_NTZ,
  trip_duration_sec   INT,
  bike_id             INT,
  rider_age           INT,
  load_ts             TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);
*
ﬁ£·◊ 7ee7020ddfe663e8e95efcbe3eeaf971
ˇ
Untitled.sql¬ALTER TASK CITIBIKE_DB.staging.t_load_staging  SUSPEND;   -- root first
ALTER TASK CITIBIKE_DB.staging.t_load_dim_station SUSPEND;
ALTER TASK CITIBIKE_DB.staging.t_load_dim_user    SUSPEND;
ALTER TASK CITIBIKE_DB.staging.t_load_fact_trips  SUSPEND;
ALTER DYNAMIC TABLE CITIBIKE_DB.mart.dt_daily_trips   SUSPEND;
ALTER DYNAMIC TABLE CITIBIKE_DB.mart.dt_station_usage SUSPEND;
ALTER DYNAMIC TABLE CITIBIKE_DB.mart.dt_station_rank  SUSPEND;

CITIBIKE_DB*
´£·◊ 1efc640793e8e14ce757d2ed1eb87efb
Ü
Data_Loading.sql≈----------------------------------------------------------------------
-- 7. LOAD DATA IN BATCHES (simulates incremental arrival)
--    Run LIST above and adjust PATTERN to match real file names.
----------------------------------------------------------------------
-- Batch 1
COPY INTO raw.trips_raw
  (tripduration, starttime, stoptime, start_station_id, start_station_latitude,
   start_station_longitude, end_station_id, end_station_latitude, end_station_longitude,
   bikeid, membership_type, usertype, birth_year, gender, src_file, src_row)
FROM (
  SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
  FROM @raw.ext_citibike_stage
)
PATTERN = '.*2020/data_0_0_.*'
ON_ERROR = 'CONTINUE';



s3://snowflake-workshop-lab/citibike-trips-csv/2020/data_0_0_0.csv.gz

SELECT COUNT(*) FROM raw.trips_raw;
SELECT SYSTEM$STREAM_HAS_DATA('raw.trips_raw_stream');   -- TRUE

-- Don't wait 5 min: trigger the graph manually
EXECUTE TASK staging.t_load_staging;

-- Batch 2 (run after batch 1 finishes flowing through)
COPY INTO raw.trips_raw
  (tripduration, starttime, stoptime, start_station_id, start_station_latitude,
   start_station_longitude, end_station_id, end_station_latitude, end_station_longitude,
   bikeid, membership_type, usertype, birth_year, gender, src_file, src_row)
FROM (
  SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
  FROM @raw.ext_citibike_stage
)
PATTERN = '.*trips_2018_2_.*'
ON_ERROR = 'CONTINUE';
-- Now let the 5-minute schedule pick it up and watch it work.*
Æ†·◊ c36ec8dd8b35b0e000ef7b4dcbc51990
‚"
	Tasks.sql®"----------------------------------------------------------------------
-- 5. TASK GRAPH  (root consumes stream; children are dependent)
--    NOTE: a stream can only be consumed once per DML, so the root task
--    drains the stream into STAGING; children read STAGING, not the stream.
----------------------------------------------------------------------
-- Root

USE DATABASE citibike_db;

CREATE OR REPLACE TASK citibike_db.staging.t_load_staging
  WAREHOUSE = practice_wh
  SCHEDULE  = '5 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('citibike_db.raw.trips_raw_stream')
AS
INSERT OVERWRITE INTO citibike_db.staging.trips_clean
SELECT
  MD5(src_file || '-' || src_row)         AS trip_id,
  tripduration, starttime, stoptime,
  start_station_id, start_station_latitude, start_station_longitude,
  end_station_id,   end_station_latitude,   end_station_longitude,
  bikeid,
  COALESCE(membership_type, 'UNKNOWN')    AS membership_type,
  COALESCE(usertype, 'UNKNOWN')           AS usertype,
  birth_year,
  COALESCE(gender, 0)                     AS gender,
  load_ts
FROM raw.trips_raw_stream
WHERE starttime IS NOT NULL
  AND tripduration > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY MD5(src_file || '-' || src_row)
                           ORDER BY load_ts) = 1;

-- Child A: station dimension
CREATE OR REPLACE TASK citibike_db.staging.t_load_dim_station
  WAREHOUSE = practice_wh
  AFTER citibike_db.staging.t_load_staging
AS
MERGE INTO citibike_db.core.dim_station d
USING (
  SELECT station_id, ANY_VALUE(lat) AS lat, ANY_VALUE(lon) AS lon
  FROM (
    SELECT start_station_id AS station_id,
           start_station_latitude AS lat, start_station_longitude AS lon
    FROM citibike_db.staging.trips_clean
    UNION ALL
    SELECT end_station_id, end_station_latitude, end_station_longitude
    FROM citibike_db.staging.trips_clean
  )
  WHERE station_id IS NOT NULL
  GROUP BY station_id
) s
ON d.station_id = s.station_id
WHEN MATCHED THEN UPDATE SET
  latitude = s.lat, longitude = s.lon, updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (station_id, latitude, longitude)
  VALUES (s.station_id, s.lat, s.lon);

-- Child B: user dimension
CREATE OR REPLACE TASK citibike_db.staging.t_load_dim_user
  WAREHOUSE = practice_wh
  AFTER citibike_db.staging.t_load_staging
AS
MERGE INTO citibike_db.core.dim_user d
USING (
  SELECT DISTINCT membership_type, usertype, gender,
         CASE gender WHEN 1 THEN 'Male' WHEN 2 THEN 'Female' ELSE 'Unknown' END AS gender_desc
  FROM citibike_db.staging.trips_clean
) s
ON  d.membership_type = s.membership_type
AND d.usertype        = s.usertype
AND d.gender          = s.gender
WHEN NOT MATCHED THEN INSERT (membership_type, usertype, gender, gender_desc)
  VALUES (s.membership_type, s.usertype, s.gender, s.gender_desc);

-- Child C: fact (waits for BOTH dimension tasks)
CREATE OR REPLACE TASK citibike_db.staging.t_load_fact_trips
  WAREHOUSE = practice_wh
  AFTER citibike_db.staging.t_load_dim_station, citibike_db.staging.t_load_dim_user
AS
MERGE INTO citibike_db.core.fact_trips f
USING (
  SELECT
    s.trip_id,
    TO_NUMBER(TO_CHAR(s.starttime::DATE,'YYYYMMDD')) AS date_key,
    ss.station_key  AS start_station_key,
    es.station_key  AS end_station_key,
    u.user_key,
    s.starttime, s.stoptime, s.tripduration, s.bikeid,
    CASE WHEN s.birth_year > 1920 THEN YEAR(s.starttime) - s.birth_year END AS rider_age
  FROM citibike_db.staging.trips_clean s
  LEFT JOIN citibike_db.core.dim_station ss ON ss.station_id = s.start_station_id
  LEFT JOIN citibike_db.core.dim_station es ON es.station_id = s.end_station_id
  JOIN citibike_db.core.dim_user u
    ON  u.membership_type = s.membership_type
    AND u.usertype        = s.usertype
    AND u.gender          = s.gender
) src
ON f.trip_id = src.trip_id
WHEN NOT MATCHED THEN INSERT
  (trip_id, date_key, start_station_key, end_station_key, user_key,
   start_ts, stop_ts, trip_duration_sec, bike_id, rider_age)
VALUES
  (src.trip_id, src.date_key, src.start_station_key, src.end_station_key, src.user_key,
   src.starttime, src.stoptime, src.tripduration, src.bikeid, src.rider_age);

-- Resume: CHILDREN FIRST, root LAST
ALTER TASK citibike_db.staging.t_load_fact_trips  RESUME;
ALTER TASK citibike_db.staging.t_load_dim_user    RESUME;
ALTER TASK citibike_db.staging.t_load_dim_station RESUME;
ALTER TASK citibike_db.staging.t_load_staging  RESUME;

show tasks;
*
≠ò·◊ a7ec7ce3473df63a347f19955f308f04
Ó
MONITORING.sqlØ----------------------------------------------------------------------
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
*
¥ó·◊ 8b01e68609bf9cdf9e9bbf44f528fc00
‹
ANALYTICS.sqlû---------------------------------------------------------------------
-- 9. ANALYTICS ON THE STAR SCHEMA / MARTS
----------------------------------------------------------------------
-- Weekday vs weekend, by user type
SELECT is_weekend, usertype, SUM(trips) AS trips, ROUND(AVG(avg_minutes),1) AS avg_min
FROM mart.dt_daily_trips
GROUP BY ALL ORDER BY 1,2;

-- Top 10 stations in each month
SELECT * FROM mart.dt_station_rank WHERE rnk <= 10 ORDER BY year, month, rnk;

-- Classic star-join: busiest days
SELECT d.full_date, d.day_name, COUNT(*) AS trips
FROM core.fact_trips f JOIN core.dim_date d ON d.date_key = f.date_key
GROUP BY ALL ORDER BY trips DESC LIMIT 10;*
´ó·◊ 5938d3717b6d0935c2430f68165cb387
’
DT.sqlû----------------------------------------------------------------------
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
*
«ï·◊ 05504fa4f5b87f0f44203e3d14a1ee76
¿
	SETUP.sqlÜUSE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;   -- needed to run tasks

USE ROLE SYSADMIN;

CREATE OR REPLACE WAREHOUSE practice_wh
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE DATABASE citibike_db;
CREATE OR REPLACE SCHEMA citibike_db.raw;
CREATE OR REPLACE SCHEMA citibike_db.staging;
CREATE OR REPLACE SCHEMA citibike_db.core;     -- star schema
CREATE OR REPLACE SCHEMA citibike_db.mart;     -- dynamic tables

USE WAREHOUSE practice_wh;
USE DATABASE citibike_db;


-- 1. FILE FORMAT + EXTERNAL STAGE (public S3, no credentials)
----------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT raw.ff_csv
  TYPE = 'CSV'
  COMPRESSION = 'AUTO'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 0
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = FALSE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  NULL_IF = ('', '\\N', 'NULL');


  -------------------------------

  CREATE OR REPLACE STAGE raw.ext_citibike_stage
  URL = 's3://snowflake-workshop-lab/citibike-trips-csv/'
  FILE_FORMAT = raw.ff_csv;

-- Verify access & see file names (pick patterns for batches below)
LIST @raw.ext_citibike_stage;


-- Peek at data before loading
SELECT $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14
FROM @raw.ext_citibike_stage (FILE_FORMAT => 'raw.ff_csv')
LIMIT 10;

*
»è·◊ 58e715bae676dfddf40e1029ac874f73