USE ROLE ACCOUNTADMIN;
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

