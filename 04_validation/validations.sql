-- =============================================================================
-- FILE: 05_validations/validations.sql
-- LAYER: SILVER — run AFTER staging.sql, BEFORE target_tables.sql
-- PURPOSE:
--   Validate the data in STG.STG_MEMBERS before it is promoted to Gold.
--   All checks output: CHECK_CATEGORY, CHECK_NAME, SEVERITY, RECORD_COUNT,
--   and SAMPLE_VALUES so issues are immediately visible.
--
-- DATA ISSUES VISIBLE IN SAMPLE FILES:
--   AUS: Mike       -> DOB is NULL
--   AUS: Jonnathan  -> Enrollment date = 2021-13-13 (month 13, impossible)
--   USA: All rows   -> Dates stored as integers (6152022), no DOB column
--   IND: Sameer     -> Born 1952, age ~72 (valid but worth flagging)
-- =============================================================================

USE DATABASE SKYPOINTS_LOYALTY_MEMBERSHIP_DB;
USE SCHEMA STG;


-- =============================================================================
-- 1. MANDATORY FIELD CHECKS
-- =============================================================================

-- 1a. Missing MEMBER_ID (primary key — critical)
SELECT
    'MANDATORY_FIELD'           AS CHECK_CATEGORY,
    'MISSING_MEMBER_ID'         AS CHECK_NAME,
    'CRITICAL'                  AS SEVERITY,
    COUNT(*)                    AS RECORD_COUNT,
    NULL                        AS SAMPLE_VALUES
FROM STG.STG_MEMBERS
WHERE MEMBER_ID IS NULL OR TRIM(MEMBER_ID) = ''
HAVING COUNT(*) > 0;


-- 1b. Missing MEMBER_NAME
SELECT
    'MANDATORY_FIELD', 'MISSING_MEMBER_NAME', 'HIGH',
    COUNT(*),
    LISTAGG(MEMBER_ID, ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS
WHERE MEMBER_NAME IS NULL OR TRIM(MEMBER_NAME) = ''
HAVING COUNT(*) > 0;


-- 1c. Missing or UNKNOWN COUNTRY
SELECT
    'MANDATORY_FIELD', 'MISSING_COUNTRY', 'CRITICAL',
    COUNT(*),
    LISTAGG(MEMBER_ID, ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS
WHERE COUNTRY IS NULL OR TRIM(COUNTRY) = '' OR COUNTRY = 'UNKNOWN'
HAVING COUNT(*) > 0;


-- 1d. Missing TIER_CODE
SELECT
    'MANDATORY_FIELD', 'MISSING_TIER_CODE', 'HIGH',
    COUNT(*),
    LISTAGG(MEMBER_ID, ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS
WHERE TIER_CODE IS NULL OR TRIM(TIER_CODE) = ''
HAVING COUNT(*) > 0;


-- 1e. Missing IS_ACTIVE
SELECT
    'MANDATORY_FIELD', 'MISSING_IS_ACTIVE', 'MEDIUM',
    COUNT(*),
    LISTAGG(MEMBER_ID, ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS
WHERE IS_ACTIVE IS NULL
HAVING COUNT(*) > 0;


-- =============================================================================
-- 2. UNIQUENESS CHECKS
-- =============================================================================

-- 2a. Duplicate MEMBER_ID within same batch and country
SELECT
    'UNIQUENESS', 'DUPLICATE_MEMBER_ID_IN_BATCH', 'HIGH',
    COUNT(*) AS DUPLICATE_GROUP_COUNT,
    LISTAGG(MEMBER_ID || ' (' || COUNTRY || ')', ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM (
    SELECT MEMBER_ID, COUNTRY, COUNT(*) AS CNT
    FROM STG.STG_MEMBERS
    GROUP BY MEMBER_ID, COUNTRY
    HAVING COUNT(*) > 1
)
HAVING COUNT(*) > 0;


-- 2b. Same MEMBER_ID appearing in multiple countries (member moved country)
SELECT
    'UNIQUENESS', 'MEMBER_IN_MULTIPLE_COUNTRIES', 'MEDIUM',
    COUNT(*),
    LISTAGG(MEMBER_ID || ': ' || COUNTRIES, ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM (
    SELECT
        MEMBER_ID,
        LISTAGG(DISTINCT COUNTRY, '/') WITHIN GROUP (ORDER BY COUNTRY) AS COUNTRIES,
        COUNT(DISTINCT COUNTRY) AS COUNTRY_COUNT
    FROM STG.STG_MEMBERS
    WHERE IS_VALID = TRUE
    GROUP BY MEMBER_ID
    HAVING COUNT(DISTINCT COUNTRY) > 1
)
HAVING COUNT(*) > 0;


-- 2c. Duplicate TXN_ID in redemptions
SELECT
    'UNIQUENESS', 'DUPLICATE_TXN_ID', 'CRITICAL',
    COUNT(*),
    LISTAGG(TXN_ID, ', ') WITHIN GROUP (ORDER BY TXN_ID)
FROM (
    SELECT TXN_ID, COUNT(*) AS CNT
    FROM STG.STG_REDEMPTIONS
    GROUP BY TXN_ID
    HAVING COUNT(*) > 1
)
HAVING COUNT(*) > 0;


-- =============================================================================
-- 3. VALUE / FORMAT CHECKS
-- =============================================================================

-- 3a. Invalid TIER_CODE
SELECT
    'DATA_FORMAT', 'INVALID_TIER_CODE', 'HIGH',
    COUNT(*),
    LISTAGG(MEMBER_ID || ':' || TIER_CODE, ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS
WHERE TIER_CODE NOT IN ('PLT', 'GLD', 'SLV', 'BRZ') OR TIER_CODE IS NULL
HAVING COUNT(*) > 0;


-- 3b. IS_ACTIVE must be 'A' or 'I'
SELECT
    'DATA_FORMAT', 'INVALID_IS_ACTIVE_VALUE', 'MEDIUM',
    COUNT(*),
    LISTAGG(MEMBER_ID || ':' || IS_ACTIVE, ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS
WHERE IS_ACTIVE IS NOT NULL AND IS_ACTIVE NOT IN ('A', 'I')
HAVING COUNT(*) > 0;


-- 3c. MEMBER_TYPE must be 'I' or 'C' when provided
SELECT
    'DATA_FORMAT', 'INVALID_MEMBER_TYPE', 'LOW',
    COUNT(*),
    LISTAGG(MEMBER_ID || ':' || MEMBER_TYPE, ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS
WHERE MEMBER_TYPE IS NOT NULL AND MEMBER_TYPE NOT IN ('I', 'C')
HAVING COUNT(*) > 0;


-- 3d. Redemption STATUS
SELECT
    'DATA_FORMAT', 'INVALID_REDEMPTION_STATUS', 'HIGH',
    COUNT(*),
    LISTAGG(TXN_ID || ':' || STATUS, ', ') WITHIN GROUP (ORDER BY TXN_ID)
FROM STG.STG_REDEMPTIONS
WHERE STATUS NOT IN ('COMPLETED', 'PENDING', 'CANCELLED') OR STATUS IS NULL
HAVING COUNT(*) > 0;


-- =============================================================================
-- 4. DATE LOGIC CHECKS
-- =============================================================================

-- 4a. Enrollment date in future
SELECT 'DATE_LOGIC', 'ENROLLMENT_DATE_IN_FUTURE', 'MEDIUM',
    COUNT(*),
    LISTAGG(MEMBER_ID || ':' || ENROLLMENT_DATE::VARCHAR, ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS WHERE ENROLLMENT_DATE > CURRENT_DATE()
HAVING COUNT(*) > 0;

-- 4b. Flight date in future
SELECT 'DATE_LOGIC', 'FLIGHT_DATE_IN_FUTURE', 'MEDIUM',
    COUNT(*),
    LISTAGG(MEMBER_ID, ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS WHERE LAST_FLIGHT_DATE > CURRENT_DATE()
HAVING COUNT(*) > 0;

-- 4c. Enrollment after last flight
SELECT 'DATE_LOGIC', 'ENROLLMENT_AFTER_FLIGHT', 'HIGH',
    COUNT(*),
    LISTAGG(MEMBER_ID || ' (ENR:' || ENROLLMENT_DATE::VARCHAR || ' FLT:' || LAST_FLIGHT_DATE::VARCHAR || ')', ', ')
        WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS
WHERE ENROLLMENT_DATE IS NOT NULL AND LAST_FLIGHT_DATE IS NOT NULL
  AND ENROLLMENT_DATE > LAST_FLIGHT_DATE
HAVING COUNT(*) > 0;

-- 4d. DOB in future
SELECT 'DATE_LOGIC', 'DOB_IN_FUTURE', 'HIGH',
    COUNT(*), LISTAGG(MEMBER_ID, ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS WHERE DOB > CURRENT_DATE()
HAVING COUNT(*) > 0;

-- 4e. Unrealistic age (outside 0-120)
SELECT 'DATE_LOGIC', 'UNREALISTIC_AGE', 'MEDIUM',
    COUNT(*),
    LISTAGG(MEMBER_ID || ' (Age:' || AGE::VARCHAR || ')', ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS WHERE AGE IS NOT NULL AND (AGE < 0 OR AGE > 120)
HAVING COUNT(*) > 0;

-- 4f. Informational: records with NULL DOB (e.g. Mike in AUS)
SELECT 'DATE_LOGIC', 'NULL_DOB_INFO', 'LOW',
    COUNT(*),
    LISTAGG(MEMBER_ID || ' (' || COUNTRY || ')', ', ') WITHIN GROUP (ORDER BY MEMBER_ID)
FROM STG.STG_MEMBERS WHERE DOB IS NULL
HAVING COUNT(*) > 0;


-- =============================================================================
-- 5. REFERENTIAL INTEGRITY
-- =============================================================================

-- 5a. Redemption with no matching member in Silver
SELECT 'REFERENTIAL_INTEGRITY', 'ORPHAN_REDEMPTION_NO_MEMBER', 'HIGH',
    COUNT(*),
    LISTAGG(r.TXN_ID || ' (member:' || r.MEMBER_ID || ')', ', ') WITHIN GROUP (ORDER BY r.TXN_ID)
FROM STG.STG_REDEMPTIONS r
LEFT JOIN STG.STG_MEMBERS m ON r.MEMBER_ID = m.MEMBER_ID AND m.IS_VALID = TRUE
WHERE m.MEMBER_ID IS NULL
HAVING COUNT(*) > 0;

-- 5b. Negative miles redeemed
SELECT 'DATA_RANGE', 'NEGATIVE_MILES_REDEEMED', 'HIGH',
    COUNT(*),
    LISTAGG(TXN_ID || ':' || MILES_REDEEMED::VARCHAR, ', ') WITHIN GROUP (ORDER BY TXN_ID)
FROM STG.STG_REDEMPTIONS WHERE MILES_REDEEMED < 0
HAVING COUNT(*) > 0;


-- =============================================================================
-- 6. SUMMARY DASHBOARD — run this to get an at-a-glance view
-- =============================================================================
SELECT CHECK_CATEGORY, CHECK_NAME, SEVERITY, RECORD_COUNT, SAMPLE_VALUES
FROM (
    SELECT 'OVERALL STATS' AS CHECK_CATEGORY, 'Total Members in Silver'      AS CHECK_NAME, 'INFO' AS SEVERITY, COUNT(*) AS RECORD_COUNT, NULL AS SAMPLE_VALUES FROM STG.STG_MEMBERS
    UNION ALL
    SELECT 'OVERALL STATS', 'Valid Members',        'INFO',    SUM(CASE WHEN IS_VALID=TRUE  THEN 1 ELSE 0 END), NULL FROM STG.STG_MEMBERS
    UNION ALL
    SELECT 'OVERALL STATS', 'Rejected Members',     'WARNING', SUM(CASE WHEN IS_VALID=FALSE THEN 1 ELSE 0 END), NULL FROM STG.STG_MEMBERS
    UNION ALL
    SELECT 'OVERALL STATS', 'Members - NULL DOB',   'INFO',    SUM(CASE WHEN DOB IS NULL THEN 1 ELSE 0 END), NULL FROM STG.STG_MEMBERS
    UNION ALL
    SELECT 'OVERALL STATS', 'Stale Members (>90d)', 'INFO',    SUM(CASE WHEN STALE_MEMBER='Y' THEN 1 ELSE 0 END), NULL FROM STG.STG_MEMBERS
    UNION ALL
    SELECT 'OVERALL STATS', 'Total Redemptions',    'INFO',    COUNT(*), NULL FROM STG.STG_REDEMPTIONS
    UNION ALL
    SELECT 'OVERALL STATS', 'Pending Redemptions',  'INFO',    SUM(CASE WHEN STATUS='PENDING' THEN 1 ELSE 0 END), NULL FROM STG.STG_REDEMPTIONS
    UNION ALL
    -- Dynamic breakdown of all recognized countries (zero hardcoding)
    SELECT 'REGIONAL BREAKDOWN', 'MEMBERS_' || COUNTRY, 'INFO', COUNT(*), NULL
    FROM STG.STG_MEMBERS
    WHERE IS_VALID = TRUE AND COUNTRY IS NOT NULL
    GROUP BY COUNTRY
)
ORDER BY
    CASE SEVERITY WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'WARNING' THEN 3 WHEN 'MEDIUM' THEN 4 WHEN 'LOW' THEN 5 ELSE 6 END,
    CHECK_CATEGORY, CHECK_NAME;


-- =============================================================================
-- 7. BATCH EXECUTION & RECONCILIATION SUMMARY
-- =============================================================================
-- Run this view to audit the overall batch load metrics and pass rates:
SELECT * FROM STG.VW_BATCH_QUALITY_SUMMARY;

-- Verify Enriched Redemptions Fact Table in Gold (Deliverable 4):
SELECT
    COUNT(*) AS TOTAL_REDEMPTION_FACT_ROWS,
    COUNT(CASE WHEN MEMBER_NAME IS NOT NULL THEN 1 END) AS MATCHED_MEMBERS,
    COUNT(CASE WHEN MEMBER_NAME IS NULL THEN 1 END)     AS UNMATCHED_ORPHAN_TXNS,
    SUM(MILES_REDEEMED) AS TOTAL_MILES_REDEEMED,
    COUNT(CASE WHEN MILES_REDEEMED <= 0 THEN 1 END)     AS LEAKED_NON_POSITIVE_MILES,
    COUNT(CASE WHEN STATUS NOT IN ('COMPLETED', 'PENDING', 'CANCELLED') THEN 1 END) AS LEAKED_INVALID_STATUS
FROM PROD.FACT_MEMBER_REDEMPTIONS;

