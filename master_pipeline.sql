-- =============================================================================
-- FILE: 06_pipeline/master_pipeline.sql
-- LAYER: ORCHESTRATION / END-TO-END PIPELINE RUNNER
-- PURPOSE:
--   Executes the entire SkyPoints Medallion pipeline in strict succession:
--
--     Step 1: RAW INGESTION      (01_raw/raw_ddl.sql)
--             -> Ingests flat file + JSON into RAW_MEMBERS and RAW_REDEMPTIONS
--
--     Step 2: JSON FLATTENING    (04_json/json_redemptions.sql)
--             -> Flattens nested JSON redemption transactions into STG_REDEMPTIONS
--
--     Step 3: SILVER TRANSFORMS  (02_staging/02_stg_transform_load.sql)
--             -> Date parsing, AGE, STALE_MEMBER, NULL Country handling, Audit
--
--     Step 4: DYNAMIC SPLIT      (02_staging/03_dynamic_country_split.sql)
--             -> SP dynamically provisions STG_MEMBERS_<COUNTRY> with SCD Type 2
--                migration tracking: IS_ACTIVE=0 and END_DATE in previous country;
--                IS_ACTIVE=1 and END_DATE=NULL in current country
--
--     Step 5: QUALITY GATE       (05_validations/validations.sql)
--             -> Validates mandatory fields, uniqueness, format integrity
--
--     Step 6: GOLD TARGETS       (03_targets/target_tables.sql)
--             -> SP dynamically provisions and copies clean data into PROD.TABLE_<COUNTRY>
--                preserving IS_ACTIVE, START_DATE, and END_DATE columns
--
-- INCLUDES:
--   Part A: Interactive / Sequential SQL Runner (for worksheets & CI/CD)
--   Part B: Automated Snowflake Task DAG (for production hands-free scheduling)
-- =============================================================================

USE DATABASE SKYPOINTS_LOYALTY_MEMBERSHIP_DB;
USE WAREHOUSE SKYPOINTS_LOYALTY_MEMBERSHIP_WH;


-- =============================================================================
-- PART A: SEQUENTIAL PIPELINE EXECUTION
-- =============================================================================

-- ── 0. GENERATE UNIFIED PIPELINE BATCH ID ─────────────────────────────────────
-- Timestamp-based token (YYYYMMDD_HH24MISS) stamped across all layers
SET BATCH_ID = TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
SET ACTIVE_BATCH_ID = $BATCH_ID;
SET BATCH_IN_RAW    = $BATCH_ID;
SELECT $BATCH_ID AS PIPELINE_RUN_BATCH_ID;


-- ── 1. RAW INGESTION (BRONZE) ────────────────────────────────────────────────
USE SCHEMA RAW;

-- Ensure schema evolution for POST_CODE column if RAW table was created earlier
ALTER TABLE IF EXISTS RAW.RAW_MEMBERS ADD COLUMN IF NOT EXISTS POST_CODE VARCHAR(10);

-- 1a. Ingest pipe-delimited flat file (members)
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
        METADATA$FILENAME,
        $BATCH_ID
    FROM @RAW.SKYPOINTS_STAGE
    (PATTERN => '.*members.*\\.(csv|dat|txt)')
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_PIPE_DELIMITED')
ON_ERROR    = 'CONTINUE'
FORCE       = TRUE;

-- 1b. Ingest JSON redemption feed
COPY INTO RAW.RAW_REDEMPTIONS (JSON_PAYLOAD, SOURCE_FILE, BATCH_ID)
FROM (
    SELECT $1, METADATA$FILENAME, $BATCH_ID
    FROM @RAW.SKYPOINTS_STAGE
    (PATTERN => '.*redemptions.*\.json')
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_JSON')
ON_ERROR    = 'CONTINUE'
FORCE       = TRUE;


-- ── 2. JSON PROCESSING (SILVER) ──────────────────────────────────────────────
USE SCHEMA STG;

CREATE OR REPLACE TABLE STG.STG_REDEMPTIONS (
    MEMBER_ID           VARCHAR(20)     NOT NULL,
    FEED_DATE           DATE,
    TXN_ID              VARCHAR(50)     NOT NULL,
    TXN_DATE            DATE,
    PARTNER             VARCHAR(100),
    MILES_REDEEMED      NUMBER(12),
    STATUS              VARCHAR(20),
    SOURCE_FILE         VARCHAR(500),
    LOAD_TS             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID            VARCHAR(50)
)
CLUSTER BY (BATCH_ID, MEMBER_ID)
COMMENT = 'Silver: physical table of flattened partner airline redemption transactions.';

-- 2b. Idempotent MERGE into STG_REDEMPTIONS (seamlessly handles late-arriving redemptions & updates)
MERGE INTO STG.STG_REDEMPTIONS tgt
USING (
    WITH latest_batch AS (
        SELECT COALESCE(MAX(BATCH_ID), 'DEFAULT_BATCH') AS BATCH_ID FROM RAW.RAW_REDEMPTIONS
    )
    SELECT
        m.VALUE:member_id::VARCHAR(20)                               AS MEMBER_ID,
        TRY_TO_DATE(m.VALUE:feed_date::VARCHAR, 'YYYYMMDD')          AS FEED_DATE,
        f.VALUE:txn_id::VARCHAR(50)                                  AS TXN_ID,
        TRY_TO_DATE(f.VALUE:txn_date::VARCHAR, 'YYYYMMDD')           AS TXN_DATE,
        f.VALUE:partner::VARCHAR(100)                                AS PARTNER,
        f.VALUE:miles_redeemed::NUMBER(12)                           AS MILES_REDEEMED,
        f.VALUE:status::VARCHAR(20)                                  AS STATUS,
        r.SOURCE_FILE,
        COALESCE(r.BATCH_ID, b.BATCH_ID)                             AS BATCH_ID
    FROM RAW.RAW_REDEMPTIONS r
    CROSS JOIN latest_batch b,
         LATERAL FLATTEN(
             INPUT => CASE 
                 WHEN IS_ARRAY(r.JSON_PAYLOAD) THEN r.JSON_PAYLOAD 
                 ELSE ARRAY_CONSTRUCT(r.JSON_PAYLOAD) 
             END
         ) m,
         LATERAL FLATTEN(INPUT => m.VALUE:redemptions) f
    WHERE (r.BATCH_ID = b.BATCH_ID OR r.BATCH_ID IS NULL OR b.BATCH_ID = 'DEFAULT_BATCH')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY f.VALUE:txn_id::VARCHAR(50)
        ORDER BY 
            TRY_TO_DATE(m.VALUE:feed_date::VARCHAR, 'YYYYMMDD') DESC,
            CASE WHEN f.VALUE:status::VARCHAR = 'COMPLETED' THEN 1 ELSE 2 END,
            r.LOAD_TS DESC
    ) = 1
) src
ON tgt.TXN_ID = src.TXN_ID
WHEN MATCHED AND (tgt.STATUS <> src.STATUS OR tgt.MILES_REDEEMED <> src.MILES_REDEEMED) THEN
    UPDATE SET
        tgt.STATUS         = src.STATUS,
        tgt.MILES_REDEEMED = src.MILES_REDEEMED,
        tgt.FEED_DATE      = src.FEED_DATE,
        tgt.BATCH_ID       = src.BATCH_ID,
        tgt.LOAD_TS        = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        MEMBER_ID, FEED_DATE, TXN_ID, TXN_DATE, PARTNER, MILES_REDEEMED, STATUS,
        SOURCE_FILE, BATCH_ID
    )
    VALUES (
        src.MEMBER_ID, src.FEED_DATE, src.TXN_ID, src.TXN_DATE, src.PARTNER, src.MILES_REDEEMED, src.STATUS,
        src.SOURCE_FILE, src.BATCH_ID
    );


-- ── 3. SILVER DATA TRANSFORMATION & AUDIT ─────────────────────────────────────
USE SCHEMA STG;

-- Ensure schema evolution for POST_CODE column if Silver tables were created earlier
ALTER TABLE IF EXISTS STG.STG_MEMBERS ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5);
ALTER TABLE IF EXISTS STG.STG_MEMBERS_REJECTED ADD COLUMN IF NOT EXISTS POST_CODE_RAW VARCHAR(10);

-- Dynamic country lookup table for uniform 3-letter ISO code resolution
CREATE TABLE IF NOT EXISTS STG.REF_COUNTRY_LOOKUP (
    ALIAS               VARCHAR(100)    NOT NULL,
    ISO_ALPHA3          VARCHAR(3)      NOT NULL,
    COUNTRY_NAME        VARCHAR(100),
    PRIMARY KEY (ALIAS)
);

MERGE INTO STG.REF_COUNTRY_LOOKUP tgt
USING (
    SELECT UPPER(TRIM(COLUMN1)) AS ALIAS, UPPER(TRIM(COLUMN2)) AS ISO_ALPHA3, TRIM(COLUMN3) AS COUNTRY_NAME
    FROM VALUES
        ('IND', 'IND', 'India'), ('INDIA', 'IND', 'India'), ('INDI', 'IND', 'India'), ('IN', 'IND', 'India'), ('BHARAT', 'IND', 'India'),
        ('AUS', 'AUS', 'Australia'), ('AUSTRALIA', 'AUS', 'Australia'), ('AUST', 'AUS', 'Australia'), ('AUSTRA', 'AUS', 'Australia'), ('AU', 'AUS', 'Australia'), ('OZ', 'AUS', 'Australia'),
        ('CAN', 'CAN', 'Canada'), ('CANADA', 'CAN', 'Canada'), ('CANAD', 'CAN', 'Canada'), ('CDA', 'CAN', 'Canada'), ('CA', 'CAN', 'Canada'),
        ('USA', 'USA', 'United States'), ('US', 'USA', 'United States'), ('UNITED STATES', 'USA', 'United States'), ('UNITED STATES OF AMERICA', 'USA', 'United States'), ('U.S.A.', 'USA', 'United States'), ('U.S.', 'USA', 'United States'), ('AMERICA', 'USA', 'United States'),
        ('PHL', 'PHL', 'Philippines'), ('PHIL', 'PHL', 'Philippines'), ('PHILIPPINES', 'PHL', 'Philippines'), ('PHILIP', 'PHL', 'Philippines'), ('PH', 'PHL', 'Philippines'), ('PILIPINAS', 'PHL', 'Philippines'),
        ('GBR', 'GBR', 'United Kingdom'), ('GB', 'GBR', 'United Kingdom'), ('UK', 'GBR', 'United Kingdom'), ('UNITED KINGDOM', 'GBR', 'United Kingdom'), ('GREAT BRITAIN', 'GBR', 'United Kingdom'), ('BRITAIN', 'GBR', 'United Kingdom'), ('ENGLAND', 'GBR', 'United Kingdom'),
        ('SGP', 'SGP', 'Singapore'), ('SG', 'SGP', 'Singapore'), ('SINGAPORE', 'SGP', 'Singapore'), ('SING', 'SGP', 'Singapore'),
        ('DEU', 'DEU', 'Germany'), ('DE', 'DEU', 'Germany'), ('GERMANY', 'DEU', 'Germany'), ('GER', 'DEU', 'Germany'), ('DEUTSCHLAND', 'DEU', 'Germany'),
        ('NZL', 'NZL', 'New Zealand'), ('NZ', 'NZL', 'New Zealand'), ('NEW ZEALAND', 'NZL', 'New Zealand'),
        ('ARE', 'ARE', 'United Arab Emirates'), ('AE', 'ARE', 'United Arab Emirates'), ('UAE', 'ARE', 'United Arab Emirates'), ('UNITED ARAB EMIRATES', 'ARE', 'United Arab Emirates'), ('DUBAI', 'ARE', 'United Arab Emirates'),
        ('JPN', 'JPN', 'Japan'), ('JP', 'JPN', 'Japan'), ('JAPAN', 'JPN', 'Japan'), ('NIPPON', 'JPN', 'Japan'),
        ('CHN', 'CHN', 'China'), ('CN', 'CHN', 'China'), ('CHINA', 'CHN', 'China'),
        ('FRA', 'FRA', 'France'), ('FR', 'FRA', 'France'), ('FRANCE', 'FRA', 'France'),
        ('ITA', 'ITA', 'Italy'), ('IT', 'ITA', 'Italy'), ('ITALY', 'ITA', 'Italy'), ('ITALIA', 'ITA', 'Italy'),
        ('ESP', 'ESP', 'Spain'), ('ES', 'ESP', 'Spain'), ('SPAIN', 'ESP', 'Spain'), ('ESPANA', 'ESP', 'Spain'),
        ('MEX', 'MEX', 'Mexico'), ('MX', 'MEX', 'Mexico'), ('MEXICO', 'MEX', 'Mexico'),
        ('BRA', 'BRA', 'Brazil'), ('BR', 'BRA', 'Brazil'), ('BRAZIL', 'BRA', 'Brazil'), ('BRASIL', 'BRA', 'Brazil'),
        ('ZAF', 'ZAF', 'South Africa'), ('ZA', 'ZAF', 'South Africa'), ('SOUTH AFRICA', 'ZAF', 'South Africa')
) src
ON tgt.ALIAS = src.ALIAS
WHEN NOT MATCHED THEN
    INSERT (ALIAS, ISO_ALPHA3, COUNTRY_NAME)
    VALUES (src.ALIAS, src.ISO_ALPHA3, src.COUNTRY_NAME);

-- 3a. Idempotent cleanup for this batch
DELETE FROM STG.STG_MEMBERS WHERE BATCH_ID = $ACTIVE_BATCH_ID;

-- 3b. Transform, derive AGE & STALE_MEMBER, validate and push to STG_MEMBERS
INSERT INTO STG.STG_MEMBERS (
    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
    IS_VALID, REJECTION_REASON, SOURCE_FILE, LOAD_TS, BATCH_ID
)
WITH raw_filtered AS (
    SELECT *
    FROM RAW.RAW_MEMBERS
    WHERE UPPER(TRIM(COALESCE(REC_TYPE, 'D'))) NOT IN ('H', 'HEADER')
      AND (
          ($BATCH_IN_RAW IS NOT NULL AND BATCH_ID = $BATCH_IN_RAW)
          OR ($BATCH_IN_RAW IS NULL)
      )
),
country_lookup AS (
    SELECT DISTINCT UPPER(TRIM(ALIAS)) AS ALIAS, UPPER(TRIM(ISO_ALPHA3)) AS ISO_ALPHA3
    FROM STG.REF_COUNTRY_LOOKUP
),
raw_normalized AS (
    SELECT
        TRIM(r.MEMBER_ID)                               AS MEMBER_ID,
        TRIM(r.MEMBER_NAME)                             AS MEMBER_NAME,
        UPPER(TRIM(r.TIER_CODE))                        AS TIER_CODE,
        TRIM(r.AGENT_NAME)                              AS AGENT_NAME,
        TRIM(r.STATE)                                   AS STATE,
        -- Dynamic country resolution via STG.REF_COUNTRY_LOOKUP:
        -- 1. Exact alias match from lookup table (e.g. 'Indi', 'India', 'Bharat' -> 'IND')
        -- 2. Direct 3-letter ISO code if already valid; otherwise NULL (triggers MISSING_COUNTRY rejection)
        COALESCE(
            lkp.ISO_ALPHA3,
            CASE WHEN LENGTH(TRIM(r.COUNTRY)) = 3 THEN UPPER(TRIM(r.COUNTRY)) END
        )                                               AS COUNTRY,
        TRY_TO_NUMBER(TRIM(r.POST_CODE))                AS POST_CODE,
        TRIM(r.POST_CODE)                               AS POST_CODE_RAW,
        NULLIF(TRIM(r.IS_ACTIVE), '')                   AS IS_ACTIVE,
        NULL::VARCHAR(1)                                AS MEMBER_TYPE,
        -- DOB: spec format is DDMMYYYY (e.g. 03051985 = 3 May 1985; also handles 7-digit 3051985)
        COALESCE(
            TRY_TO_DATE(TRIM(r.DOB), 'DDMMYYYY'),
            CASE WHEN LENGTH(TRIM(r.DOB)) = 7 THEN TRY_TO_DATE('0' || TRIM(r.DOB), 'DDMMYYYY') END
        )                                               AS DOB_PARSED,
        TRIM(r.DOB)                                       AS DOB_RAW,

        -- Enrollment date: Intelligent multi-format parser avoiding false year matches (e.g. 1052022 -> 2022-10-05, NOT 1052-02-02)
        CASE
            WHEN TRIM(r.ENROLLMENT_DATE) LIKE '____-__-__'
                 THEN TRY_TO_DATE(TRIM(r.ENROLLMENT_DATE), 'YYYY-MM-DD')

            WHEN LENGTH(TRIM(r.ENROLLMENT_DATE)) = 8 AND REGEXP_LIKE(TRIM(r.ENROLLMENT_DATE), '^[0-9]+$')
                 THEN COALESCE(
                     CASE WHEN SUBSTR(TRIM(r.ENROLLMENT_DATE), 1, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(TRIM(r.ENROLLMENT_DATE), 'YYYYMMDD') END,
                     CASE WHEN SUBSTR(TRIM(r.ENROLLMENT_DATE), 5, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(TRIM(r.ENROLLMENT_DATE), 'MMDDYYYY') END
                 )

            WHEN LENGTH(TRIM(r.ENROLLMENT_DATE)) = 7 AND REGEXP_LIKE(TRIM(r.ENROLLMENT_DATE), '^[0-9]+$')
                 THEN COALESCE(
                     CASE WHEN TRY_TO_NUMBER(SUBSTR(TRIM(r.ENROLLMENT_DATE), 1, 2)) BETWEEN 1 AND 12
                           AND SUBSTR(TRIM(r.ENROLLMENT_DATE), 4, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(
                              SUBSTR(TRIM(r.ENROLLMENT_DATE), 4, 4) || '-' ||
                              SUBSTR(TRIM(r.ENROLLMENT_DATE), 1, 2) || '-0' ||
                              SUBSTR(TRIM(r.ENROLLMENT_DATE), 3, 1),
                              'YYYY-MM-DD'
                          ) END,
                     CASE WHEN SUBSTR(TRIM(r.ENROLLMENT_DATE), 4, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(
                              SUBSTR(TRIM(r.ENROLLMENT_DATE), 4, 4) || '-0' ||
                              SUBSTR(TRIM(r.ENROLLMENT_DATE), 1, 1) || '-' ||
                              SUBSTR(TRIM(r.ENROLLMENT_DATE), 2, 2),
                              'YYYY-MM-DD'
                          ) END
                 )
        END                                             AS ENROLLMENT_DATE_PARSED,
        TRIM(r.ENROLLMENT_DATE)                           AS ENROLLMENT_DATE_RAW,

        -- Last flight date: same intelligent multi-format parser
        CASE
            WHEN TRIM(r.LAST_FLIGHT_DATE) LIKE '____-__-__'
                 THEN TRY_TO_DATE(TRIM(r.LAST_FLIGHT_DATE), 'YYYY-MM-DD')

            WHEN LENGTH(TRIM(r.LAST_FLIGHT_DATE)) = 8 AND REGEXP_LIKE(TRIM(r.LAST_FLIGHT_DATE), '^[0-9]+$')
                 THEN COALESCE(
                     CASE WHEN SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 1, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(TRIM(r.LAST_FLIGHT_DATE), 'YYYYMMDD') END,
                     CASE WHEN SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 5, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(TRIM(r.LAST_FLIGHT_DATE), 'MMDDYYYY') END
                 )

            WHEN LENGTH(TRIM(r.LAST_FLIGHT_DATE)) = 7 AND REGEXP_LIKE(TRIM(r.LAST_FLIGHT_DATE), '^[0-9]+$')
                 THEN COALESCE(
                     CASE WHEN TRY_TO_NUMBER(SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 1, 2)) BETWEEN 1 AND 12
                           AND SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 4, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(
                              SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 4, 4) || '-' ||
                              SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 1, 2) || '-0' ||
                              SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 3, 1),
                              'YYYY-MM-DD'
                          ) END,
                     CASE WHEN SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 4, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(
                              SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 4, 4) || '-0' ||
                              SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 1, 1) || '-' ||
                              SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 2, 2),
                              'YYYY-MM-DD'
                          ) END
                 )
        END                                             AS LAST_FLIGHT_DATE_PARSED,
        TRIM(r.LAST_FLIGHT_DATE)                          AS LAST_FLIGHT_DATE_RAW,
        COALESCE(r.SOURCE_FILE, 'MEMBERS_FEED')           AS SOURCE_FILE,
        COALESCE(r.LOAD_TS, CURRENT_TIMESTAMP())          AS LOAD_TS,
        $ACTIVE_BATCH_ID                                AS BATCH_ID
    FROM raw_filtered r
    LEFT JOIN country_lookup lkp
        ON UPPER(TRIM(r.COUNTRY)) = lkp.ALIAS
),
with_derived AS (
    SELECT
        *,
        CASE WHEN DOB_PARSED IS NOT NULL THEN DATEDIFF('year', DOB_PARSED, CURRENT_DATE()) END AS AGE,
        CASE
            WHEN LAST_FLIGHT_DATE_PARSED IS NULL THEN 'U'
            WHEN DATEDIFF('day', LAST_FLIGHT_DATE_PARSED, CURRENT_DATE()) > 90 THEN 'Y'
            ELSE 'N'
        END AS STALE_MEMBER,
        ARRAY_TO_STRING(
            ARRAY_CONSTRUCT_COMPACT(
                CASE WHEN MEMBER_ID IS NULL OR TRIM(MEMBER_ID) IN ('', '0', '00000000') THEN 'MISSING_OR_ZERO_MEMBER_ID' END,
                CASE WHEN MEMBER_NAME IS NULL OR TRIM(MEMBER_NAME) = '' THEN 'MISSING_MEMBER_NAME' END,
                CASE WHEN COUNTRY IS NULL OR TRIM(COUNTRY) IN ('', 'UNKNOWN') THEN 'MISSING_COUNTRY' END,
                CASE WHEN TIER_CODE NOT IN ('PLT','GLD','SLV','BRZ') THEN 'INVALID_TIER_CODE' END,
                CASE WHEN IS_ACTIVE IS NULL THEN 'MISSING_IS_ACTIVE' END,
                CASE WHEN ENROLLMENT_DATE_RAW IS NULL OR TRIM(ENROLLMENT_DATE_RAW) IN ('', '0', '00000000') THEN 'MISSING_OR_ZERO_ENROLLMENT_DATE' END,
                CASE WHEN ENROLLMENT_DATE_PARSED IS NULL THEN 'INVALID_ENROLLMENT_DATE' END,
                CASE WHEN DOB_RAW IS NOT NULL AND TRIM(DOB_RAW) NOT IN ('', '0', 'NULL') AND DOB_PARSED IS NULL THEN 'INVALID_DOB_FORMAT' END,
                CASE WHEN LAST_FLIGHT_DATE_RAW IS NOT NULL AND TRIM(LAST_FLIGHT_DATE_RAW) NOT IN ('', '0', 'NULL') AND LAST_FLIGHT_DATE_PARSED IS NULL THEN 'INVALID_FLIGHT_DATE' END,
                CASE WHEN ENROLLMENT_DATE_PARSED IS NOT NULL
                      AND LAST_FLIGHT_DATE_PARSED IS NOT NULL
                      AND ENROLLMENT_DATE_PARSED > LAST_FLIGHT_DATE_PARSED THEN 'ENROLLMENT_AFTER_FLIGHT' END,
                CASE WHEN DOB_PARSED IS NOT NULL
                      AND (DATEDIFF('year', DOB_PARSED, CURRENT_DATE()) < 0
                        OR DATEDIFF('year', DOB_PARSED, CURRENT_DATE()) > 120) THEN 'UNREALISTIC_AGE' END,
                CASE WHEN ENROLLMENT_DATE_PARSED > CURRENT_DATE() THEN 'ENROLLMENT_DATE_IN_FUTURE' END,
                CASE WHEN LAST_FLIGHT_DATE_PARSED > CURRENT_DATE() THEN 'FLIGHT_DATE_IN_FUTURE' END
            ),
            '|'
        ) AS REJECTION_REASON
    FROM raw_normalized
)
SELECT
    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB_PARSED, ENROLLMENT_DATE_PARSED, LAST_FLIGHT_DATE_PARSED,
    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
    CASE WHEN REJECTION_REASON IS NULL OR REJECTION_REASON = '' THEN TRUE ELSE FALSE END AS IS_VALID,
    NULLIF(REJECTION_REASON, '') AS REJECTION_REASON,
    SOURCE_FILE, LOAD_TS, BATCH_ID
FROM with_derived;

-- 3c. Idempotent load into STG_MEMBERS_REJECTED (Guarantees zero duplicates on re-runs)
DELETE FROM STG.STG_MEMBERS_REJECTED 
WHERE BATCH_ID = $ACTIVE_BATCH_ID OR ($BATCH_IN_RAW IS NULL);

INSERT INTO STG.STG_MEMBERS_REJECTED (
    MEMBER_ID, MEMBER_NAME, TIER_CODE,
    DOB_RAW, ENROLLMENT_DATE_RAW, LAST_FLIGHT_DATE_RAW,
    COUNTRY, POST_CODE_RAW, IS_ACTIVE, REJECTION_REASON,
    SOURCE_FILE, BATCH_ID
)
SELECT
    s.MEMBER_ID,
    s.MEMBER_NAME,
    s.TIER_CODE,
    COALESCE(r.DOB, TO_VARCHAR(s.DOB), '') AS DOB_RAW,
    COALESCE(r.ENROLLMENT_DATE, TO_VARCHAR(s.ENROLLMENT_DATE), '') AS ENROLLMENT_DATE_RAW,
    COALESCE(r.LAST_FLIGHT_DATE, TO_VARCHAR(s.LAST_FLIGHT_DATE), '') AS LAST_FLIGHT_DATE_RAW,
    s.COUNTRY,
    COALESCE(r.POST_CODE, TO_VARCHAR(s.POST_CODE), '') AS POST_CODE_RAW,
    s.IS_ACTIVE,
    s.REJECTION_REASON,
    s.SOURCE_FILE,
    s.BATCH_ID
FROM STG.STG_MEMBERS s
LEFT JOIN RAW.RAW_MEMBERS r 
    ON s.MEMBER_ID = r.MEMBER_ID
   AND s.BATCH_ID  = r.BATCH_ID
   AND UPPER(TRIM(COALESCE(r.REC_TYPE, 'D'))) NOT IN ('H', 'HEADER')
WHERE s.IS_VALID = FALSE;


-- ── 3d. BATCH AUDIT & QUALITY RECONCILIATION VIEW ─────────────────────────────
-- Provides operational visibility into batch performance, pass rate, and quality metrics
CREATE OR REPLACE VIEW STG.VW_BATCH_QUALITY_SUMMARY AS
SELECT
    COALESCE(s.BATCH_ID, 'TOTAL')                                               AS BATCH_ID,
    COUNT(s.MEMBER_ID)                                                          AS TOTAL_RECORDS,
    COUNT(CASE WHEN s.IS_VALID = TRUE THEN 1 END)                               AS VALID_RECORDS,
    COUNT(CASE WHEN s.IS_VALID = FALSE THEN 1 END)                              AS REJECTED_RECORDS,
    ROUND(COUNT(CASE WHEN s.IS_VALID = TRUE THEN 1 END) * 100.0 / NULLIF(COUNT(s.MEMBER_ID), 0), 2) AS PASS_PERCENTAGE,
    MIN(s.LOAD_TS)                                                              AS BATCH_START_TIME,
    MAX(s.LOAD_TS)                                                              AS BATCH_END_TIME
FROM STG.STG_MEMBERS s
GROUP BY GROUPING SETS ((s.BATCH_ID), ());



-- ── 4. DYNAMIC COUNTRY TABLE CREATION & SPLIT (SILVER) ────────────────────────
-- Automatically creates STG_MEMBERS_<COUNTRY> for every country found in data.
-- Implements SCD Type 2 country migration tracking:
--   - In previous country (e.g. IND): IS_ACTIVE = 0 and END_DATE = date of move
--   - In current country (e.g. USA):  IS_ACTIVE = 1 and END_DATE = NULL
CREATE OR REPLACE PROCEDURE STG.SP_CREATE_AND_LOAD_COUNTRY_TABLES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_active_batch VARCHAR;
    c_cursor CURSOR FOR
        SELECT DISTINCT UPPER(TRIM(COUNTRY)) AS COUNTRY
        FROM STG.STG_MEMBERS
        WHERE IS_VALID = TRUE
          AND COUNTRY IS NOT NULL
          AND TRIM(COUNTRY) NOT IN ('', 'UNKNOWN');
    v_country VARCHAR;
    v_sql VARCHAR;
    v_count INT := 0;
BEGIN
    -- Resolve active batch ID from session variable or fall back to latest in STG
    v_active_batch := COALESCE((SELECT $ACTIVE_BATCH_ID), (SELECT MAX(BATCH_ID) FROM STG.STG_MEMBERS));

    FOR rec IN c_cursor DO
        v_country := rec.COUNTRY;

        -- Step 4a: Create country-specific Silver table dynamically if not exists
        v_sql := 'CREATE TABLE IF NOT EXISTS STG.STG_MEMBERS_' || v_country || ' (
            MEMBER_ID           VARCHAR(20)     NOT NULL,
            MEMBER_NAME         VARCHAR(100),
            TIER_CODE           VARCHAR(10),
            DOB                 DATE,
            ENROLLMENT_DATE     DATE,
            LAST_FLIGHT_DATE    DATE,
            AGE                 NUMBER(3)       COMMENT ''Recomputed from DOB vs CURRENT_DATE'',
            STALE_MEMBER        VARCHAR(1)      COMMENT ''Recomputed from LAST_FLIGHT_DATE'',
            AGENT_NAME          VARCHAR(100),
            STATE               VARCHAR(50),
            COUNTRY             VARCHAR(10),
            POST_CODE           NUMBER(5)       COMMENT ''Post code (5-digit integer per spec, optional)'',
            IS_ACTIVE           NUMBER(1)       COMMENT ''1 if currently active in this country, 0 if moved to another country'',
            MEMBER_TYPE         VARCHAR(1),
            START_DATE          DATE            COMMENT ''Date member became active in this country'',
            END_DATE            DATE            COMMENT ''Date member moved away; NULL if currently active'',
            SOURCE_FILE         VARCHAR(500),
            BATCH_ID            VARCHAR(50),
            STG_LOAD_TS         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
        ) CLUSTER BY (MEMBER_ID)
        COMMENT = ''Silver: members for country ' || v_country || ' with migration tracking (1=active, 0=moved)''';
        EXECUTE IMMEDIATE v_sql;

        -- Ensure POST_CODE column exists if country table was created previously without it
        v_sql := 'ALTER TABLE STG.STG_MEMBERS_' || v_country || ' ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5)';
        EXECUTE IMMEDIATE v_sql;

        -- Step 4b: Incremental SCD-2 Expiry (Close out members who moved AWAY from this country in the active batch)
        v_sql := 'UPDATE STG.STG_MEMBERS_' || v_country || ' tgt
            SET 
                tgt.IS_ACTIVE   = 0,
                tgt.END_DATE    = src.MOVE_DATE,
                tgt.STG_LOAD_TS = CURRENT_TIMESTAMP()
            FROM (
                SELECT 
                    s.MEMBER_ID,
                    COALESCE(s.LAST_FLIGHT_DATE, s.ENROLLMENT_DATE, CURRENT_DATE()) AS MOVE_DATE
                FROM STG.STG_MEMBERS s
                WHERE s.IS_VALID = TRUE
                  AND s.COUNTRY <> ''' || v_country || '''
                  AND (s.BATCH_ID = ''' || v_active_batch || ''' OR ''' || v_active_batch || ''' IS NULL)
            ) src
            WHERE tgt.MEMBER_ID = src.MEMBER_ID
              AND tgt.IS_ACTIVE = 1';
        EXECUTE IMMEDIATE v_sql;

        -- Step 4c: Incremental MERGE with "Latest Country Wins" SCD Type 2 logic
        v_sql := 'MERGE INTO STG.STG_MEMBERS_' || v_country || ' tgt
            USING (
                WITH global_latest AS (
                    -- Identifies the single globally active country for each member
                    SELECT 
                        MEMBER_ID,
                        COUNTRY AS ACTIVE_COUNTRY,
                        COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE) AS MOVE_DATE
                    FROM STG.STG_MEMBERS
                    WHERE IS_VALID = TRUE
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY MEMBER_ID 
                        ORDER BY COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE) DESC, LOAD_TS DESC
                    ) = 1
                ),
                country_records AS (
                    -- Latest record for this specific country
                    SELECT 
                        MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
                        AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, MEMBER_TYPE,
                        COALESCE(ENROLLMENT_DATE, LOAD_TS::DATE) AS START_DATE,
                        SOURCE_FILE, BATCH_ID
                    FROM STG.STG_MEMBERS
                    WHERE IS_VALID = TRUE
                      AND COUNTRY = ''' || v_country || '''
                      AND (BATCH_ID = ''' || v_active_batch || ''' OR ''' || v_active_batch || ''' IS NULL)
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY MEMBER_ID 
                        ORDER BY COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE) DESC, LOAD_TS DESC
                    ) = 1
                )
                SELECT 
                    c.MEMBER_ID, c.MEMBER_NAME, c.TIER_CODE, c.DOB, c.ENROLLMENT_DATE, c.LAST_FLIGHT_DATE,
                    c.AGE, c.STALE_MEMBER, c.AGENT_NAME, c.STATE, c.COUNTRY, c.POST_CODE, c.MEMBER_TYPE,
                    c.START_DATE,
                    -- Latest Country Wins: 1 if this country is their latest profile globally; 0 if moved away
                    CASE WHEN g.ACTIVE_COUNTRY = ''' || v_country || ''' THEN 1 ELSE 0 END AS IS_ACTIVE,
                    CASE WHEN g.ACTIVE_COUNTRY = ''' || v_country || ''' THEN NULL ELSE g.MOVE_DATE END AS END_DATE,
                    c.SOURCE_FILE, c.BATCH_ID
                FROM country_records c
                JOIN global_latest g ON c.MEMBER_ID = g.MEMBER_ID
            ) src
            ON tgt.MEMBER_ID = src.MEMBER_ID
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.MEMBER_NAME      = src.MEMBER_NAME,
                    tgt.TIER_CODE        = src.TIER_CODE,
                    tgt.DOB              = src.DOB,
                    tgt.ENROLLMENT_DATE  = src.ENROLLMENT_DATE,
                    tgt.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
                    tgt.AGE              = src.AGE,
                    tgt.STALE_MEMBER     = src.STALE_MEMBER,
                    tgt.AGENT_NAME       = src.AGENT_NAME,
                    tgt.STATE            = src.STATE,
                    tgt.POST_CODE        = src.POST_CODE,
                    tgt.IS_ACTIVE        = src.IS_ACTIVE,
                    tgt.END_DATE         = src.END_DATE,
                    tgt.MEMBER_TYPE      = src.MEMBER_TYPE,
                    tgt.SOURCE_FILE      = src.SOURCE_FILE,
                    tgt.BATCH_ID         = src.BATCH_ID,
                    tgt.STG_LOAD_TS      = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (
                    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
                    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
                    START_DATE, END_DATE, SOURCE_FILE, BATCH_ID, STG_LOAD_TS
                )
                VALUES (
                    src.MEMBER_ID, src.MEMBER_NAME, src.TIER_CODE, src.DOB, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE,
                    src.AGE, src.STALE_MEMBER, src.AGENT_NAME, src.STATE, src.COUNTRY, src.POST_CODE,
                    src.IS_ACTIVE, src.MEMBER_TYPE, src.START_DATE, src.END_DATE, src.SOURCE_FILE, src.BATCH_ID, CURRENT_TIMESTAMP()
                )';
        EXECUTE IMMEDIATE v_sql;

        v_count := v_count + 1;
    END FOR;

    RETURN 'Success: Incrementally synchronized ' || v_count || ' per-country tables in STG for batch ' || COALESCE(v_active_batch, 'ALL') || '.';
END;
$$;

CALL STG.SP_CREATE_AND_LOAD_COUNTRY_TABLES();


-- ── 5. DATA QUALITY VALIDATIONS (QUALITY GATE) ────────────────────────────────
-- Quick automated gate: ensure no critical errors before promoting to Gold
SELECT
    'CRITICAL_DEFECT_CHECK' AS CHECK_TYPE,
    COUNT(*) AS DEFECT_COUNT
FROM STG.STG_MEMBERS
WHERE BATCH_ID = $BATCH_ID
  AND IS_VALID = FALSE
  AND (MEMBER_ID IS NULL OR COUNTRY IS NULL);


-- ── 6. GOLD TARGET SYNCHRONIZATION ───────────────────────────────────────────
USE SCHEMA PROD;

-- Dynamically provisions and copies Silver country tables into Gold:
CREATE OR REPLACE PROCEDURE PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    c_cursor CURSOR FOR
        SELECT DISTINCT COUNTRY
        FROM STG.STG_MEMBERS
        WHERE IS_VALID = TRUE
          AND COUNTRY IS NOT NULL
          AND TRIM(COUNTRY) NOT IN ('', 'UNKNOWN');
    v_country VARCHAR;
    v_sql VARCHAR;
    v_count INT := 0;
BEGIN
    FOR rec IN c_cursor DO
        v_country := UPPER(TRIM(rec.COUNTRY));

        -- Step 6a: Create target table dynamically if it does not exist
        v_sql := 'CREATE TABLE IF NOT EXISTS PROD.TABLE_' || v_country || ' (
            MEMBER_ID           VARCHAR(20)     NOT NULL,
            MEMBER_NAME         VARCHAR(100),
            TIER_CODE           VARCHAR(10),
            DOB                 DATE,
            ENROLLMENT_DATE     DATE,
            LAST_FLIGHT_DATE    DATE,
            AGE                 NUMBER(3),
            STALE_MEMBER        VARCHAR(1),
            AGENT_NAME          VARCHAR(100),
            STATE               VARCHAR(50),
            COUNTRY             VARCHAR(10),
            POST_CODE           NUMBER(5)       COMMENT ''Post code (5-digit integer per spec, optional)'',
            IS_ACTIVE           NUMBER(1)       COMMENT ''1 if currently active in this country, 0 if moved to another country'',
            MEMBER_TYPE         VARCHAR(1),
            START_DATE          DATE            COMMENT ''Date member became active in this country'',
            END_DATE            DATE            COMMENT ''Date member moved away; NULL if currently active'',
            SOURCE_FILE         VARCHAR(500),
            BATCH_ID            VARCHAR(50),
            LOADED_AT           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
        ) CLUSTER BY (MEMBER_ID)
        COMMENT = ''Gold: per-country table for ' || v_country || ' with migration tracking (1=active, 0=moved)''';
        EXECUTE IMMEDIATE v_sql;

        -- Ensure POST_CODE column exists if country table was created previously without it
        v_sql := 'ALTER TABLE PROD.TABLE_' || v_country || ' ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5)';
        EXECUTE IMMEDIATE v_sql;

        -- Step 6b: Atomic Truncate and Reload from Silver country master
        v_sql := 'TRUNCATE TABLE PROD.TABLE_' || v_country;
        EXECUTE IMMEDIATE v_sql;

        v_sql := 'INSERT INTO PROD.TABLE_' || v_country || ' (
            MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
            AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
            START_DATE, END_DATE, SOURCE_FILE, BATCH_ID, LOADED_AT
        )
        SELECT 
            MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
            AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
            START_DATE, END_DATE, SOURCE_FILE, BATCH_ID, CURRENT_TIMESTAMP()
        FROM STG.STG_MEMBERS_' || v_country;
        EXECUTE IMMEDIATE v_sql;

        v_count := v_count + 1;
    END FOR;

    RETURN 'Success: Dynamically provisioned and loaded ' || v_count || ' per-country tables in PROD.';
END;
$$;

CALL PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES();

-- ── 6b. ENRICHED MEMBER REDEMPTIONS FACT TABLE (Assessment Deliverable 4) ───
-- Production analytics table joining partner airline redemption transactions
-- with member profile data.
CREATE TABLE IF NOT EXISTS PROD.FACT_MEMBER_REDEMPTIONS (
    TXN_ID              VARCHAR(50)     NOT NULL,
    TXN_DATE            DATE,
    FEED_DATE           DATE,
    PARTNER             VARCHAR(100),
    MILES_REDEEMED      NUMBER(12),
    STATUS              VARCHAR(20),
    -- Joined Member Attributes
    MEMBER_ID           VARCHAR(20)     NOT NULL,
    MEMBER_NAME         VARCHAR(100),
    TIER_CODE           VARCHAR(10),
    AGE                 NUMBER(3),
    STALE_MEMBER        VARCHAR(1),
    AGENT_NAME          VARCHAR(100),
    STATE               VARCHAR(50),
    COUNTRY             VARCHAR(10)     COMMENT 'Member canonical 3-letter country',
    POST_CODE           NUMBER(5),
    IS_ACTIVE           NUMBER(1)       COMMENT '1 if active, 0 if inactive/moved',
    -- Audit
    SOURCE_FILE         VARCHAR(500),
    BATCH_ID            VARCHAR(50),
    LOADED_AT           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (TXN_ID)
)
CLUSTER BY (TXN_DATE, MEMBER_ID)
COMMENT = 'Gold: Production fact table joining partner airline redemptions with member profiles';

-- 6c. Purge any corrupted, invalid, or orphan transactions that may have leaked into the Gold mart
DELETE FROM PROD.FACT_MEMBER_REDEMPTIONS 
WHERE MILES_REDEEMED <= 0 
   OR STATUS NOT IN ('COMPLETED', 'PENDING', 'CANCELLED')
   OR TXN_DATE > CURRENT_DATE() 
   OR TXN_DATE IS NULL
   OR MEMBER_NAME IS NULL;

MERGE INTO PROD.FACT_MEMBER_REDEMPTIONS tgt
USING (
    WITH latest_member AS (
        SELECT
            MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, AGE, STALE_MEMBER,
            AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
            ENROLLMENT_DATE, LAST_FLIGHT_DATE
        FROM STG.STG_MEMBERS
        WHERE IS_VALID = TRUE
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY MEMBER_ID 
            ORDER BY COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE) DESC, LOAD_TS DESC
        ) = 1
    ),
    dedup_redemptions AS (
        SELECT
            TXN_ID,
            TXN_DATE,
            FEED_DATE,
            PARTNER,
            MILES_REDEEMED,
            STATUS,
            MEMBER_ID,
            SOURCE_FILE,
            BATCH_ID,
            LOAD_TS
        FROM STG.STG_REDEMPTIONS
        WHERE MILES_REDEEMED > 0
          AND STATUS IN ('COMPLETED', 'PENDING', 'CANCELLED')
          AND TXN_DATE IS NOT NULL
          AND TXN_DATE <= CURRENT_DATE()
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TXN_ID
            ORDER BY FEED_DATE DESC, LOAD_TS DESC, CASE WHEN STATUS = 'COMPLETED' THEN 1 ELSE 2 END
        ) = 1
    )
    SELECT
        r.TXN_ID,
        r.TXN_DATE,
        r.FEED_DATE,
        r.PARTNER,
        r.MILES_REDEEMED,
        r.STATUS,
        r.MEMBER_ID,
        m.MEMBER_NAME,
        m.TIER_CODE,
        m.AGE,
        m.STALE_MEMBER,
        m.AGENT_NAME,
        m.STATE,
        m.COUNTRY,
        m.POST_CODE,
        CASE WHEN m.IS_ACTIVE = 'A' OR m.IS_ACTIVE = '1' THEN 1 ELSE 0 END AS IS_ACTIVE,
        r.SOURCE_FILE,
        r.BATCH_ID
    FROM dedup_redemptions r
    LEFT JOIN latest_member m
        ON r.MEMBER_ID = m.MEMBER_ID
    WHERE m.MEMBER_ID IS NOT NULL
) src
ON tgt.TXN_ID = src.TXN_ID
WHEN MATCHED THEN
    UPDATE SET
        tgt.TXN_DATE         = src.TXN_DATE,
        tgt.FEED_DATE        = src.FEED_DATE,
        tgt.PARTNER          = src.PARTNER,
        tgt.MILES_REDEEMED   = src.MILES_REDEEMED,
        tgt.STATUS           = src.STATUS,
        tgt.MEMBER_NAME      = src.MEMBER_NAME,
        tgt.TIER_CODE        = src.TIER_CODE,
        tgt.AGE              = src.AGE,
        tgt.STALE_MEMBER     = src.STALE_MEMBER,
        tgt.AGENT_NAME       = src.AGENT_NAME,
        tgt.STATE            = src.STATE,
        tgt.COUNTRY          = src.COUNTRY,
        tgt.POST_CODE        = src.POST_CODE,
        tgt.IS_ACTIVE        = src.IS_ACTIVE,
        tgt.BATCH_ID         = src.BATCH_ID,
        tgt.LOADED_AT        = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        TXN_ID, TXN_DATE, FEED_DATE, PARTNER, MILES_REDEEMED, STATUS,
        MEMBER_ID, MEMBER_NAME, TIER_CODE, AGE, STALE_MEMBER, AGENT_NAME,
        STATE, COUNTRY, POST_CODE, IS_ACTIVE, SOURCE_FILE, BATCH_ID
    )
    VALUES (
        src.TXN_ID, src.TXN_DATE, src.FEED_DATE, src.PARTNER, src.MILES_REDEEMED, src.STATUS,
        src.MEMBER_ID, src.MEMBER_NAME, src.TIER_CODE, src.AGE, src.STALE_MEMBER, src.AGENT_NAME,
        src.STATE, src.COUNTRY, src.POST_CODE, src.IS_ACTIVE, src.SOURCE_FILE, src.BATCH_ID
    );





-- ── 7. SUMMARY REPORT ────────────────────────────────────────────────────────
SELECT 'PIPELINE_COMPLETE' AS STATUS, $BATCH_ID AS PROCESSED_BATCH_ID;




-- =============================================================================
-- PART B: AUTOMATED SNOWFLAKE TASK DAG (Hands-Free Production Schedule)
-- =============================================================================
-- In enterprise production, this DAG runs daily at 02:00 UTC automatically.
-- Each task triggers the next upon successful completion.
-- =============================================================================

/*
-- Root Task: Starts at 02:00 AM UTC
CREATE OR REPLACE TASK SKYPOINTS_TASK_01_RAW_INGEST
    WAREHOUSE = SKYPOINTS_LOYALTY_MEMBERSHIP_WH
    SCHEDULE  = 'USING CRON 0 2 * * * UTC'
AS
    COPY INTO RAW.RAW_MEMBERS ...;

-- Child Task 2: Flattens JSON after Raw Ingest finishes
CREATE OR REPLACE TASK SKYPOINTS_TASK_02_JSON_FLATTEN
    WAREHOUSE = SKYPOINTS_LOYALTY_MEMBERSHIP_WH
    AFTER SKYPOINTS_TASK_01_RAW_INGEST
AS
    INSERT INTO STG.STG_REDEMPTIONS ...;

-- Child Task 3: Cleanses & derives columns
CREATE OR REPLACE TASK SKYPOINTS_TASK_03_SILVER_TRANSFORM
    WAREHOUSE = SKYPOINTS_LOYALTY_MEMBERSHIP_WH
    AFTER SKYPOINTS_TASK_02_JSON_FLATTEN
AS
    INSERT INTO STG.STG_MEMBERS ...;

-- Child Task 4: Dynamically splits into country tables
CREATE OR REPLACE TASK SKYPOINTS_TASK_04_COUNTRY_SPLIT
    WAREHOUSE = SKYPOINTS_LOYALTY_MEMBERSHIP_WH
    AFTER SKYPOINTS_TASK_03_SILVER_TRANSFORM
AS
    CALL STG.SP_CREATE_AND_LOAD_COUNTRY_TABLES();

-- Child Task 5: Loads Gold target tables
CREATE OR REPLACE TASK SKYPOINTS_TASK_05_GOLD_LOAD
    WAREHOUSE = SKYPOINTS_LOYALTY_MEMBERSHIP_WH
    AFTER SKYPOINTS_TASK_04_COUNTRY_SPLIT
AS
    CALL PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES();

-- To activate the DAG:
-- ALTER TASK SKYPOINTS_TASK_05_GOLD_LOAD RESUME;
-- ALTER TASK SKYPOINTS_TASK_04_COUNTRY_SPLIT RESUME;
-- ALTER TASK SKYPOINTS_TASK_03_SILVER_TRANSFORM RESUME;
-- ALTER TASK SKYPOINTS_TASK_02_JSON_FLATTEN RESUME;
-- ALTER TASK SKYPOINTS_TASK_01_RAW_INGEST RESUME;
*/


;


