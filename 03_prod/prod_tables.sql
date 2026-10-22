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

        -- Step 2: Incremental MERGE from Silver country table into Gold (Zero TRUNCATE)
        v_sql := 'MERGE INTO PROD.TABLE_' || v_country || ' tgt
            USING (
                SELECT * FROM STG.STG_MEMBERS_' || v_country || '
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY MEMBER_ID 
                    ORDER BY START_DATE DESC, STG_LOAD_TS DESC
                ) = 1
            ) src
            ON tgt.MEMBER_ID = src.MEMBER_ID
            WHEN MATCHED AND (
                tgt.IS_ACTIVE        <> src.IS_ACTIVE OR
                COALESCE(tgt.END_DATE, ''1900-01-01'') <> COALESCE(src.END_DATE, ''1900-01-01'') OR
                tgt.TIER_CODE        <> src.TIER_CODE OR
                COALESCE(tgt.LAST_FLIGHT_DATE, ''1900-01-01'') <> COALESCE(src.LAST_FLIGHT_DATE, ''1900-01-01'') OR
                COALESCE(tgt.POST_CODE, 0) <> COALESCE(src.POST_CODE, 0)
            ) THEN
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
                    tgt.MEMBER_TYPE      = src.MEMBER_TYPE,
                    tgt.START_DATE       = src.START_DATE,
                    tgt.END_DATE         = src.END_DATE,
                    tgt.SOURCE_FILE      = src.SOURCE_FILE,
                    tgt.BATCH_ID         = src.BATCH_ID,
                    tgt.LOADED_AT        = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (
                    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
                    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
                    START_DATE, END_DATE, SOURCE_FILE, BATCH_ID, LOADED_AT
                )
                VALUES (
                    src.MEMBER_ID, src.MEMBER_NAME, src.TIER_CODE, src.DOB, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE,
                    src.AGE, src.STALE_MEMBER, src.AGENT_NAME, src.STATE, src.COUNTRY, src.POST_CODE,
                    src.IS_ACTIVE, src.MEMBER_TYPE, src.START_DATE, src.END_DATE, src.SOURCE_FILE, src.BATCH_ID, CURRENT_TIMESTAMP()
                )';
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
