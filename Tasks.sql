----------------------------------------------------------------------
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
