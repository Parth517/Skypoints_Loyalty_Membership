USE DATABASE SKYPOINTS_LOYALTY_MEMBERSHIP_DB;
USE SCHEMA STG;

-- ── 1. STG_REDEMPTIONS PHYSICAL TABLE DDL ────────────────────────────────────
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


-- ── 2. IDEMPOTENT MERGE INTO STG_REDEMPTIONS (HANDLES LATE-ARRIVING DATA) ───
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

-- =============================================================================
-- 3. JOINING REDEMPTIONS TO MEMBER PROFILE DATA (Assessment Deliverable 4)
-- =============================================================================
-- The assessment requires describing and demonstrating how redemption data
-- joins back to member profile data.
--
-- DESIGN DECISIONS:
-- 1. Grain: Each row represents one distinct redemption transaction (TXN_ID).
-- 2. Join Key: MEMBER_ID.
-- 3. Profile Version: Joins against the member's deduplicated valid profile
--    to enrich transactions with Member Name, Tier, Country, State, Agent, and Age.
-- =============================================================================

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
    m.COUNTRY AS MEMBER_COUNTRY,
    m.IS_ACTIVE
FROM STG.STG_REDEMPTIONS r
LEFT JOIN (
    SELECT *
    FROM STG.STG_MEMBERS
    WHERE IS_VALID = TRUE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY MEMBER_ID 
        ORDER BY COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE) DESC, LOAD_TS DESC
    ) = 1
) m ON r.MEMBER_ID = m.MEMBER_ID
WHERE r.MILES_REDEEMED > 0
  AND r.STATUS IN ('COMPLETED', 'PENDING', 'CANCELLED')
  AND r.TXN_DATE IS NOT NULL
  AND r.TXN_DATE <= CURRENT_DATE()
  AND m.MEMBER_ID IS NOT NULL;