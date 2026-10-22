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