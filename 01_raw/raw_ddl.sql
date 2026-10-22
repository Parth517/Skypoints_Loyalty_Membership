-- =============================================================================
-- FILE: 01_raw/raw_ddl.sql
-- LAYER: RAW / LANDING
-- PURPOSE:
--   - Creates the database, warehouse, and schema
--   - Creates an internal stage to land uploaded files
--   - Creates RAW_MEMBERS table (flat file ingestion) -- all VARCHAR
--   - Creates RAW_REDEMPTIONS table (JSON ingestion) -- VARIANT
--
-- DESIGN PRINCIPLES:
--   - NO transformations at this layer. Store exactly what the source sends.
--   - All flat-file columns are VARCHAR to survive bad dates, nulls, integers, etc.
--   - Metadata columns (LOAD_TS, SOURCE_FILE, BATCH_ID) added for auditability.
--   - This layer is append-only; never update or delete from RAW.
-- =============================================================================


-- -------------------------------------------------
-- 1. DATABASE / WAREHOUSE / SCHEMA SETUP
-- -------------------------------------------------
CREATE DATABASE IF NOT EXISTS SKYPOINTS_LOYALTY_MEMBERSHIP_DB;

CREATE WAREHOUSE IF NOT EXISTS SKYPOINTS_LOYALTY_MEMBERSHIP_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Warehouse for SkyPoints loyalty ETL';

USE DATABASE SKYPOINTS_LOYALTY_MEMBERSHIP_DB;
USE WAREHOUSE SKYPOINTS_LOYALTY_MEMBERSHIP_WH;

CREATE SCHEMA IF NOT EXISTS RAW    COMMENT = 'Landing zone - raw, unmodified source data';
CREATE SCHEMA IF NOT EXISTS STG    COMMENT = 'Staging - cleansed, normalized, with derived columns';
CREATE SCHEMA IF NOT EXISTS PROD   COMMENT = 'Production - per-country target tables';

USE SCHEMA RAW;


-- -------------------------------------------------
-- 2. FILE FORMAT -- Pipe-Delimited Flat File
-- -------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS RAW.FF_PIPE_DELIMITED
    TYPE                           = 'CSV'
    FIELD_DELIMITER                = '|'
    SKIP_HEADER                    = 1
    NULL_IF                        = ('NULL', 'null', 'N/A', '', 'NA')
    EMPTY_FIELD_AS_NULL            = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'Pipe-delimited format for SkyPoints flat file feed';


-- -------------------------------------------------
-- 3. FILE FORMAT -- JSON Feed
-- -------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS RAW.FF_JSON
    TYPE              = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMMENT           = 'JSON format for partner airline redemption feed (strips outer array)';


-- -------------------------------------------------
-- 4. INTERNAL STAGE
-- -------------------------------------------------
-- Use IF NOT EXISTS so uploaded files are preserved across re-runs
CREATE STAGE IF NOT EXISTS RAW.SKYPOINTS_STAGE
    COMMENT = 'Internal stage for SkyPoints daily feeds';


-- -------------------------------------------------
-- 5. RAW MEMBERS TABLE
-- -------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW.RAW_MEMBERS (
    REC_TYPE            VARCHAR(1)      COMMENT 'H=header D=detail',
    MEMBER_NAME         VARCHAR(100),
    MEMBER_ID           VARCHAR(20),
    ENROLLMENT_DATE     VARCHAR(20),
    LAST_FLIGHT_DATE    VARCHAR(20),
    TIER_CODE           VARCHAR(10),
    AGENT_NAME          VARCHAR(100),
    STATE               VARCHAR(50),
    COUNTRY             VARCHAR(10),
    POST_CODE           VARCHAR(10)     COMMENT 'Post code (5-digit integer per spec, optional)',
    DOB                 VARCHAR(20),
    IS_ACTIVE           VARCHAR(1),
    -- Audit columns
    SOURCE_FILE         VARCHAR(500)    COMMENT 'Name of the loaded source file',
    LOAD_TS             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID            VARCHAR(50)     COMMENT 'Daily batch ID e.g. 20240115_001'
)
COMMENT = 'Landing table for SkyPoints flat file feed. Append-only.';


-- -------------------------------------------------
-- 6. RAW REDEMPTIONS TABLE
-- -------------------------------------------------
CREATE TABLE IF NOT EXISTS RAW.RAW_REDEMPTIONS (
    JSON_PAYLOAD        VARIANT         COMMENT 'Full JSON object from partner airline',
    SOURCE_FILE         VARCHAR(500),
    LOAD_TS             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID            VARCHAR(50)
)
COMMENT = 'Landing table for JSON redemption feed. Append-only.';


-- =============================================================================
-- 7. COPY INTO COMMANDS
-- =============================================================================
-- DYNAMIC BATCH_ID GENERATION (Option B - Timestamp Run Token):
--   Format: YYYYMMDD_HH24MISS (e.g. 20261019_193000)
--   - Generated dynamically at pipeline start using CURRENT_TIMESTAMP()
--   - 100% unique every single run without requiring manual intervention
--   - Acts as a constant logical token attached to every row in this batch
-- =============================================================================

SET BATCH_ID = TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');

-- Verify before loading:
SELECT $BATCH_ID AS ACTIVE_BATCH_ID;


-- 7a. Flat file load
-- Before running: PUT file:///local/path/members_<date>.dat @RAW.SKYPOINTS_STAGE;
COPY INTO RAW.RAW_MEMBERS (
    REC_TYPE, MEMBER_NAME, MEMBER_ID, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE,
    SOURCE_FILE, BATCH_ID
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8,
        $9,                                              -- COUNTRY
        CASE WHEN $12 IS NOT NULL THEN $10 ELSE $12 END, -- POST_CODE (if 12 cols, $10 is Post Code; if 11 cols, NULL)
        CASE WHEN $12 IS NOT NULL THEN $11 ELSE $10 END, -- DOB
        CASE WHEN $12 IS NOT NULL THEN $12 ELSE $11 END, -- IS_ACTIVE
        METADATA$FILENAME,   -- Snowflake auto-populates the source file name
        $BATCH_ID            -- dynamic: YYYYMMDD_RRR, set above
    FROM @RAW.SKYPOINTS_STAGE
    -- Pattern matches any flat file in the stage for today's date
    (PATTERN => '.*members.*\\.(csv|dat|txt)')
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_PIPE_DELIMITED')
ON_ERROR    = 'CONTINUE'
FORCE       = TRUE;   -- allow re-loading during testing/re-runs


-- 7b. JSON feed load
-- Before running: PUT file:///local/path/redemptions_<date>.json @RAW.SKYPOINTS_STAGE;
COPY INTO RAW.RAW_REDEMPTIONS (JSON_PAYLOAD, SOURCE_FILE, BATCH_ID)
FROM (
    SELECT
        $1,
        METADATA$FILENAME,
        $BATCH_ID
    FROM @RAW.SKYPOINTS_STAGE
    (PATTERN => '.*redemptions.*\.json')
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_JSON')
ON_ERROR    = 'CONTINUE'
FORCE       = TRUE;
