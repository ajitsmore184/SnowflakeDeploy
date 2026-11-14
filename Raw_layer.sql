----------------------------------------------------------------------
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
