-- =============================================================================
-- FILE: 03_target/target_tables.sql
-- LAYER: GOLD / PRODUCTION
-- PURPOSE:
--   Dynamic per-country table provisioning and enriched fact mart loading.
--   ZERO static/hardcoded regional table declarations.
--
--   1. Dynamic Per-Country Table Provisioning:
--      Stored procedure PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES() queries all
--      distinct countries present in Silver (STG.STG_MEMBERS), dynamically
--      provisions PROD.TABLE_<COUNTRY> with clustering, and populates it
--      directly from the corresponding Silver country table (STG_MEMBERS_<COUNTRY>).
--      Scales to any number of countries (IND, USA, AUS, CAN, GBR, etc.)
--      without manual DDL intervention.
--
--   2. Enriched Member Redemptions Fact Mart:
--      PROD.FACT_MEMBER_REDEMPTIONS joins partner airline redemption transactions
--      with conformed member profiles via idempotent MERGE.
-- =============================================================================

USE DATABASE SKYPOINTS_LOYALTY_MEMBERSHIP_DB;
USE SCHEMA PROD;


-- =============================================================================
-- 1. DYNAMIC PER-COUNTRY TABLE GENERATION & REFRESH (ALL Countries)
-- =============================================================================
-- Stored Procedure queries distinct countries present in STG.STG_MEMBERS,
-- dynamically creates PROD.TABLE_<COUNTRY> if not already existing, and
-- performs a zero-business-logic direct copy from STG.STG_MEMBERS_<COUNTRY>.
-- =============================================================================

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

        -- Step 1: Create target table dynamically if it does not exist
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

        -- Step 2: Atomic Truncate and Reload from Silver country master
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


-- =============================================================================
-- 2. EXECUTE & VERIFY
-- =============================================================================

-- Run the dynamic procedure (creates and populates TABLE_USA, TABLE_IND, TABLE_AUS, TABLE_CAN, TABLE_PHL, etc.):
CALL PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES();

-- =============================================================================
-- 3. ENRICHED MEMBER REDEMPTIONS FACT MART (Assessment Deliverable 4)
-- =============================================================================
-- Production analytics table joining partner airline redemption transactions
-- with member profile data.
-- =============================================================================

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

-- Idempotent MERGE into FACT_MEMBER_REDEMPTIONS
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