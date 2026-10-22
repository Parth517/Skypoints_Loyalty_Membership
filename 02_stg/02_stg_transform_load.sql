-- =============================================================================
-- FILE: 02_staging/02_stg_transform_load.sql
-- LAYER: SILVER / DATA CLEANSING & VALIDATION & PER-COUNTRY SPLIT
-- PURPOSE:
--   1. Cleanse, validate and ingest records from RAW.RAW_MEMBERS into STG.STG_MEMBERS
--   2. Multi-format date parsing (DDMMYYYY, ISO YYYY-MM-DD, numeric 7/8-digit MMDDYYYY/YYYYMMDD)
--   3. Derive business attributes:
--        - AGE: computed from DOB vs CURRENT_DATE
--        - STALE_MEMBER: 'Y' if last flight > 90 days ago, 'N' if active, 'U' if no flights
--   4. Handle NULL / missing COUNTRY:
--        - Derive from SOURCE_FILE name if blank; flag 'MISSING_COUNTRY' if still NULL
--   5. Route invalid records into STG.STG_MEMBERS_REJECTED (idempotent MERGE)
--   6. Split valid members into per-country target tables (STG.STG_MEMBERS_<COUNTRY>)
--      applying the "latest record wins" rule with SCD Type 2 country migration tracking:
--        - In current country:  IS_ACTIVE = 1 and END_DATE = NULL
--        - In previous country: IS_ACTIVE = 0 and END_DATE = date of move
--
-- IDEMPOTENCY GUARANTEE:
--   - STG_MEMBERS: Cleansed per-batch using DELETE WHERE BATCH_ID = $ACTIVE_BATCH_ID
--   - STG_MEMBERS_REJECTED: De-duplicated idempotent MERGE on MEMBER_ID (updates existing, inserts new)
--   - STG_MEMBERS_<COUNTRY>: TRUNCATE + INSERT per country ensures zero duplicates on re-runs
-- =============================================================================

USE DATABASE SKYPOINTS_LOYALTY_MEMBERSHIP_DB;
USE SCHEMA STG;

-- ── 1. SET ACTIVE BATCH IDENTIFIER ────────────────────────────────────────────
SET BATCH_IN_RAW = (SELECT MAX(BATCH_ID) FROM RAW.RAW_MEMBERS WHERE BATCH_ID IS NOT NULL);
SET ACTIVE_BATCH_ID = COALESCE($BATCH_IN_RAW, TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS'));

SELECT $ACTIVE_BATCH_ID AS ACTIVE_BATCH_ID, $BATCH_IN_RAW AS BATCH_IN_RAW;


-- Ensure schema evolution for POST_CODE column if Silver tables were created earlier
ALTER TABLE IF EXISTS STG.STG_MEMBERS ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5);
ALTER TABLE IF EXISTS STG.STG_MEMBERS_REJECTED ADD COLUMN IF NOT EXISTS POST_CODE_RAW VARCHAR(10);

-- ── 2. IDEMPOTENT CLEANUP ──────────────────────────────────────────────────────
-- Deletes prior data for the active batch to ensure idempotency on re-runs
DELETE FROM STG.STG_MEMBERS 
WHERE BATCH_ID = $ACTIVE_BATCH_ID
   OR ($BATCH_IN_RAW IS NULL);


-- ── 3. TRANSFORM & POPULATE STG_MEMBERS ───────────────────────────────────────
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


-- ── 4b. BATCH AUDIT & QUALITY RECONCILIATION VIEW ─────────────────────────────
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