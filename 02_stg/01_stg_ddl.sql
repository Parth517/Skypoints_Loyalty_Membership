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



-- ── 3. REF_COUNTRY_LOOKUP: Dynamic country resolution lookup table ────────────
CREATE TABLE IF NOT EXISTS STG.REF_COUNTRY_LOOKUP (
    ALIAS               VARCHAR(100)    NOT NULL,
    ISO_ALPHA3          VARCHAR(3)      NOT NULL,
    COUNTRY_NAME        VARCHAR(100),
    PRIMARY KEY (ALIAS)
)
COMMENT = 'Silver: Reference table mapping country names, prefixes, abbreviations, and ISO codes to canonical 3-letter ISO code.';

-- Populate lookup table with comprehensive seed mappings (idempotent MERGE)
MERGE INTO STG.REF_COUNTRY_LOOKUP tgt
USING (
    SELECT UPPER(TRIM(COLUMN1)) AS ALIAS, UPPER(TRIM(COLUMN2)) AS ISO_ALPHA3, TRIM(COLUMN3) AS COUNTRY_NAME
    FROM VALUES
        -- India
        ('IND', 'IND', 'India'), ('INDIA', 'IND', 'India'), ('INDI', 'IND', 'India'), ('IN', 'IND', 'India'), ('BHARAT', 'IND', 'India'),
        -- Australia
        ('AUS', 'AUS', 'Australia'), ('AUSTRALIA', 'AUS', 'Australia'), ('AUST', 'AUS', 'Australia'), ('AUSTRA', 'AUS', 'Australia'), ('AU', 'AUS', 'Australia'), ('OZ', 'AUS', 'Australia'),
        -- Canada
        ('CAN', 'CAN', 'Canada'), ('CANADA', 'CAN', 'Canada'), ('CANAD', 'CAN', 'Canada'), ('CDA', 'CAN', 'Canada'), ('CA', 'CAN', 'Canada'),
        -- United States
        ('USA', 'USA', 'United States'), ('US', 'USA', 'United States'), ('UNITED STATES', 'USA', 'United States'), ('UNITED STATES OF AMERICA', 'USA', 'United States'), ('U.S.A.', 'USA', 'United States'), ('U.S.', 'USA', 'United States'), ('AMERICA', 'USA', 'United States'),
        -- Philippines
        ('PHL', 'PHL', 'Philippines'), ('PHIL', 'PHL', 'Philippines'), ('PHILIPPINES', 'PHL', 'Philippines'), ('PHILIP', 'PHL', 'Philippines'), ('PH', 'PHL', 'Philippines'), ('PILIPINAS', 'PHL', 'Philippines'),
        -- United Kingdom
        ('GBR', 'GBR', 'United Kingdom'), ('GB', 'GBR', 'United Kingdom'), ('UK', 'GBR', 'United Kingdom'), ('UNITED KINGDOM', 'GBR', 'United Kingdom'), ('GREAT BRITAIN', 'GBR', 'United Kingdom'), ('BRITAIN', 'GBR', 'United Kingdom'), ('ENGLAND', 'GBR', 'United Kingdom'),
        -- Singapore
        ('SGP', 'SGP', 'Singapore'), ('SG', 'SGP', 'Singapore'), ('SINGAPORE', 'SGP', 'Singapore'), ('SING', 'SGP', 'Singapore'),
        -- Germany
        ('DEU', 'DEU', 'Germany'), ('DE', 'DEU', 'Germany'), ('GERMANY', 'DEU', 'Germany'), ('GER', 'DEU', 'Germany'), ('DEUTSCHLAND', 'DEU', 'Germany'),
        -- New Zealand
        ('NZL', 'NZL', 'New Zealand'), ('NZ', 'NZL', 'New Zealand'), ('NEW ZEALAND', 'NZL', 'New Zealand')
) src
ON tgt.ALIAS = src.ALIAS
WHEN NOT MATCHED THEN
    INSERT (ALIAS, ISO_ALPHA3, COUNTRY_NAME)
    VALUES (src.ALIAS, src.ISO_ALPHA3, src.COUNTRY_NAME);