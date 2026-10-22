-- =============================================================================
-- FILE: 02_staging/01_stg_ddl.sql
-- LAYER: SILVER / STAGING DDL
-- PURPOSE:
--   Creates core tables in STG schema:
--     1. STG_MEMBERS          - Append-only history of all members across all countries
--     2. STG_MEMBERS_REJECTED - Audit table for invalid records
--
--   Note: Country tables (STG_MEMBERS_USA, STG_MEMBERS_IND, etc.) are NOT hardcoded
--   here. They are created dynamically by 03_dynamic_country_split.sql!
-- =============================================================================

USE DATABASE SKYPOINTS_LOYALTY_MEMBERSHIP_DB;
USE SCHEMA STG;

-- ── 1. STG_MEMBERS: Full history landing table ──────────────────────────────
CREATE OR REPLACE TABLE STG.STG_MEMBERS (
    MEMBER_ID               VARCHAR(20),
    MEMBER_NAME             VARCHAR(100),
    TIER_CODE               VARCHAR(10),
    DOB                     DATE,
    ENROLLMENT_DATE         DATE,
    LAST_FLIGHT_DATE        DATE,
    AGE                     NUMBER(3)       COMMENT 'Calculated age at time of batch load',
    STALE_MEMBER            VARCHAR(1)      COMMENT 'Y if last flight > 90 days at batch load',
    AGENT_NAME              VARCHAR(100),
    STATE                   VARCHAR(50),
    COUNTRY                 VARCHAR(10)     COMMENT 'May be NULL if not provided in source feed',
    POST_CODE               NUMBER(5)       COMMENT '5-digit postal code (optional per spec: INT, Mandatory=N)',
    IS_ACTIVE               VARCHAR(1)      COMMENT 'A=Active, I/N=Inactive',
    MEMBER_TYPE             VARCHAR(1)      COMMENT 'I=Individual, C=Corporate',
    IS_VALID                BOOLEAN         COMMENT 'TRUE if passed all data quality validations',
    REJECTION_REASON        VARCHAR(2000)   COMMENT 'Pipe-delimited rejection reasons if invalid',
    SOURCE_FILE             VARCHAR(500),
    LOAD_TS                 TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID                VARCHAR(50)
)
CLUSTER BY (BATCH_ID, MEMBER_ID)
COMMENT = 'Stage: validated, cleansed and strongly typed staging table for member records';


-- ── 2. STG_MEMBERS_REJECTED: Audit trail for bad data ───────────────────────
CREATE OR REPLACE TABLE STG.STG_MEMBERS_REJECTED (
    MEMBER_ID               VARCHAR(20),
    MEMBER_NAME             VARCHAR(100),
    TIER_CODE               VARCHAR(10),
    DOB_RAW                 VARCHAR(20),
    ENROLLMENT_DATE_RAW     VARCHAR(20),
    LAST_FLIGHT_DATE_RAW    VARCHAR(20),
    COUNTRY                 VARCHAR(10),
    POST_CODE_RAW           VARCHAR(10),
    IS_ACTIVE               VARCHAR(1),
    REJECTION_REASON        VARCHAR(2000),
    SOURCE_FILE             VARCHAR(500),
    BATCH_ID                VARCHAR(50),
    REJECTED_AT             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (BATCH_ID)
COMMENT = 'Stage: Dead Letter Queue (DLQ) preserving unparsed rejected records with reason codes.';