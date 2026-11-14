----------------------------------------------------------------------
-- 7. LOAD DATA IN BATCHES (simulates incremental arrival)
--    Run LIST above and adjust PATTERN to match real file names.
----------------------------------------------------------------------
-- Batch 1 (2021 files use a different layout: duration in minutes, no lat/long,
-- text bike id/gender/DOB). COPY can't do arithmetic, so INSERT from the stage.
INSERT INTO citibike_db.raw.trips_raw
  (tripduration, starttime, stoptime, start_station_id, start_station_latitude,
   start_station_longitude, end_station_id, end_station_latitude, end_station_longitude,
   bikeid, membership_type, usertype, birth_year, gender, src_file, src_row)
SELECT TRY_TO_NUMBER($4) * 60, TRY_TO_TIMESTAMP_NTZ($2), TRY_TO_TIMESTAMP_NTZ($3),
       TRY_TO_NUMBER($5), NULL, NULL, TRY_TO_NUMBER($6), NULL, NULL,
       TRY_TO_NUMBER(REPLACE($7,'-','')),
       $13,
       IFF($13 = 'annual member', 'Subscriber', 'Customer'),
       YEAR(TRY_TO_DATE($11)),
       DECODE($12, 'male', 1, 'female', 2, 0),
       METADATA$FILENAME, METADATA$FILE_ROW_NUMBER
FROM @citibike_db.raw.ext_citibike_stage
     (FILE_FORMAT => 'citibike_db.raw.ff_csv', PATTERN => '.*2021/data_0_0_.*');


--EXECUTE TASK staging.t_ingest_raw;

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
-- Now let the 5-minute schedule pick it up and watch it work.