# SkyPoints Platform Architecture and Technical Design

> **Enterprise Medallion Architecture on Snowflake**  
> *Dynamic Regional Partitioning, SCD Type 2 Residency Tracking, and Audit Governance*

---

## 1. Architectural Blueprint

The platform implements an automated Medallion Architecture (Bronze to Silver to Gold) on Snowflake with schema isolation, dynamic schema provisioning, resilient quality enforcement, and an isolated dead letter audit store.

```text
+----------------------------------------------------------------------------------------------------+
|                                    INBOUND SOURCE FEEDS                                            |
|                  Flat Files (|D|Elena|223457|...)  &  JSON Feeds ({"member_id": ...})              |
+--------------------------------------------------+-------------------------------------------------+
                                                   |
                                                   v
+----------------------------------------------------------------------------------------------------+
| 1. BRONZE LAYER (RAW Schema) - Immutable Landing Zone                                              |
|                                                                                                    |
|  [@RAW.SKYPOINTS_STAGE] -> Internal / External Cloud Storage Stage                                 |
|                                                                                                    |
|  +------------------------------------------------+  +-------------------------------------------+ |
|  | RAW.RAW_MEMBERS                                |  | RAW.RAW_REDEMPTIONS                       | |
|  | - All VARCHAR schema-on-read landing           |  | - Immutable VARIANT JSON store            | |
|  | - Preserves raw strings and absorbs drift      |  | - Retains full raw payload integrity      | |
|  +-----------------------+------------------------+  +---------------------+---------------------+ |
+--------------------------|-------------------------------------------------|-----------------------+
                           |                                                 |
                           |                                                 v
                           |                               +-----------------------------------------+
                           |                               | LATERAL FLATTEN on JSON Payloads        |
                           |                               +--------------------+--------------------+
                           |                                                    |
                           v                                                    v
+----------------------------------------------------------------------------------------------------+
| 2. SILVER LAYER (STG Schema) - Cleansing, Normalization and Staging                                 |
|                                                                                                    |
|  [STG.REF_COUNTRY_LOOKUP] -> Dynamic ISO Alpha-3 Geographic Reference and Alias Catalog            |
|                                                                                                    |
|  +------------------------------------------------+  +-------------------------------------------+ |
|  | STG.STG_MEMBERS                                |  | STG.STG_REDEMPTIONS                       | |
|  | - Multi-branch date parsing (DDMMYYYY, ISO)    |  | - Relational transaction grain            | |
|  | - Dropped leading zero fix (3051985 -> 03051985)|  | - Idempotent MERGE on TXN_ID              | |
|  | - Derived AGE and STALE_MEMBER metrics         |  | - Status updates (PENDING -> COMPLETED)   | |
|  | - Geographic alias resolution via lookup table |  | - Prevents duplicates across re-runs      | |
|  +-----------------------+------------------------+  +---------------------+---------------------+ |
+--------------------------|-------------------------------------------------|-----------------------+
                           |                                                 |
            +--------------+--------------+                                  |
            |                             |                                  |
     [IS_VALID = TRUE]            [IS_VALID = FALSE]                         |
            |                             |                                  |
            v                             v                                  |
+------------------------------+  +------------------------------+           |
| DYNAMIC COUNTRY SPLIT (STG)  |  | AUDIT / REJECTION STORE (DLQ)|           |
|                              |  |                              |           |
| SP_CREATE_AND_LOAD_          |  | STG.STG_MEMBERS_REJECTED     |           |
| COUNTRY_TABLES()             |  | - Isolates corrupt records   |           |
|                              |  | - Preserves raw text values  |           |
| Dynamically provisions:      |  | - Pipe-delimited error codes:|           |
| STG.STG_MEMBERS_<COUNTRY>    |  |   MISSING_ID, INVALID_DOB,   |           |
| (STG_MEMBERS_IND, USA, etc.) |  |   ENROLLMENT_AFTER_FLIGHT,   |           |
|                              |  |   FUTURE_DATE, BAD_COUNTRY   |           |
| SCD Type 2 Migration Track:  |  +------------------------------+           |
| - Current Country: ACTIVE=1  |                                             |
| - Previous Country: ACTIVE=0 |                                             |
| - START_DATE and END_DATE    |                                             |
+--------------+---------------+                                             |
               |                                                             |
               v                                                             |
+----------------------------------------------------------------------------|-----------------------+
| 3. GOLD LAYER (PROD Schema) - Curated Consumption Marts                    |                       |
|                                                                            |                       |
|  +-----------------------------------------------+  +----------------------v---------------------+ |
|  | PROD.TABLE_<COUNTRY>                          |  | PROD.FACT_MEMBER_REDEMPTIONS               | |
|  |                                               |  |                                            | |
|  | Synchronized via:                             |  | - Enriched analytical fact table           | |
|  | PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES()         |  | - Joins redemptions with member profiles   | |
|  |                                               |  | - Transaction grain: TXN_ID, TXN_DATE      | |
|  | Direct copies from Silver country tables:     |  | - Enriched with Member Name, Tier, Age,    | |
|  | - PROD.TABLE_IND                              |  |   Stale Status, State, Country, Active     | |
|  | - PROD.TABLE_USA                              |  | - Clustered by (TXN_DATE, MEMBER_ID)       | |
|  | - PROD.TABLE_AUS                              |  | - Powers executive and partner reporting   | |
|  | - PROD.TABLE_CAN                              |  |                                            | |
|  | - PROD.TABLE_PHL                              |  |                                            | |
|  |                                               |  |                                            | |
|  | Clustered by (MEMBER_ID) for fast lookups     |  |                                            | |
|  +-----------------------------------------------+  +--------------------------------------------+ |
+----------------------------------------------------------------------------------------------------+
```

---

## 2. Layer Specifications

### 2.1 Bronze Layer (Landing Zone - `RAW` Schema)
- Storage Abstraction:
  - `@RAW.SKYPOINTS_STAGE`: Secure landing stage supporting daily pipe-delimited flat files and nested JSON transaction payloads.
  - File Formats: `RAW.FF_PIPE_DELIMITED` (with lenient delimiter handling) and `RAW.FF_JSON` (with outer array stripping).
- Schema-on-Read Resilience:
  - `RAW.RAW_MEMBERS`: Ingests all inbound attributes as loose `VARCHAR` data types. This prevents ingestion aborts when encountering unexpected text in dates, non-numeric postal codes, or shifted columns.
  - `RAW.RAW_REDEMPTIONS`: Ingests partner transaction payloads into a native `VARIANT` document store, preserving the raw JSON structure without schema restriction.
- Auditability and Lineage:
  - Captures `METADATA$FILENAME` into `SOURCE_FILE`, current timestamp into `LOAD_TS`, and pipeline run identifier into `BATCH_ID`.

### 2.2 Silver Layer (Cleansing, Normalization and Staging - `STG` Schema)
- Dynamic Country Resolution (`STG.REF_COUNTRY_LOOKUP`):
  - Decouples geographic normalization from query logic. Pre-seeded with canonical ISO 3166-1 alpha-3 codes (`IND`, `AUS`, `USA`, `CAN`, `PHL`, `GBR`, etc.) and comprehensive regional aliases (`Indi`, `Aust`, `US`, `PHIL`, `UK`).
  - Implements multi-tier resolution: Level 1 (Exact Alias Match) -> Level 2 (Canonical Prefix Match via `STARTSWITH`) -> Level 3 (Direct 3-letter pass-through) -> Unmatched values return `NULL`.
- Type Casting and Derived Attributes:
  - Multi-branch date parsing parses `DDMMYYYY`, `YYYY-MM-DD`, and integer dates (`MMDDYYYY`), with specific handling for 7-digit birth dates missing leading zeros (`3051985` -> `03051985`).
  - `AGE`: Calculated dynamically by evaluating birth date against `CURRENT_DATE()`.
  - `STALE_MEMBER`: Derived operational indicator (`'Y'` if last flight > 90 days ago, `'N'` if flight occurred within 90 days, `'U'` if no flight history).
- JSON Flattening and Transaction Ingestion (`STG.STG_REDEMPTIONS`):
  - Utilizes `LATERAL FLATTEN` on `redemptions` arrays to convert nested documents into relational records.
  - Employs an idempotent `MERGE` on transaction primary key `TXN_ID`, enabling seamless late-arriving inserts and status updates (`PENDING` -> `COMPLETED`) without duplicating records.
- Dead Letter Queue Audit Store (`STG.STG_MEMBERS_REJECTED`):
  - Records failing business rules (missing IDs, invalid dates, future timestamps, enrollment after flight, unmapped countries) receive `IS_VALID = FALSE`.
  - Corrupt rows are isolated into `STG.STG_MEMBERS_REJECTED` with preserved raw strings and pipe-delimited diagnostic failure codes.
- Dynamic Territory Provisioning and SCD Type 2 Migration:
  - `STG.SP_CREATE_AND_LOAD_COUNTRY_TABLES()` queries distinct valid normalized countries, dynamically creates `STG.STG_MEMBERS_<COUNTRY>` tables, and truncates/reloads them.
  - Applies analytical window functions (`ROW_NUMBER()` and `LEAD()`):
    - Historical country records receive `IS_ACTIVE = 0` and `END_DATE = date of move`.
    - Current active country records receive `IS_ACTIVE = 1` and `END_DATE = NULL`.

### 2.3 Gold Layer (Curated Consumption - `PROD` Schema)
- Per-Country Reporting Tables (`PROD.TABLE_<COUNTRY>`):
  - Synchronized via stored procedure `PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES()`.
  - Zero transformation in Gold: direct copy from Silver country tables ensures maximum throughput and deterministic reporting.
  - Clustered by `(MEMBER_ID)` to optimize point lookups.
- Enriched Fact Table (`PROD.FACT_MEMBER_REDEMPTIONS`):
  - Star-schema consumption fact table joining partner redemption transactions with member profile data.
  - Attributes include: `TXN_ID`, `TXN_DATE`, `FEED_DATE`, `PARTNER`, `MILES_REDEEMED`, `STATUS`, `MEMBER_ID`, `MEMBER_NAME`, `TIER_CODE`, `AGE`, `STALE_MEMBER`, `STATE`, `COUNTRY`, `POST_CODE`, `MEMBER_IS_ACTIVE`.
  - Clustered on `(TXN_DATE, MEMBER_ID)` for sub-second analytical dashboarding.

---

## 3. Orchestration and Automation Workflow

The platform provides dual-mode orchestration for development flexibility and production automation:

```
                            Snowflake Orchestrator
                                      |
         +----------------------------+----------------------------+
         |                                                         |
         v                                                         v
   Interactive Runner                                     Automated Task DAG
  (master_pipeline.sql)                                 (Crontab: 02:00 UTC Daily)
         |                                                         |
         +-----------------------+---------------------------------+
                                 |
                                 v
        +-------------------------------------------------+
        | Step 0: Generate Batch Token ($BATCH_ID)        |
        +------------------------+------------------------+
                                 |
                                 v
        +-------------------------------------------------+
        | Step 1: Raw Ingestion (COPY INTO Members & JSON)|
        +------------------------+------------------------+
                                 |
                                 v
        +-------------------------------------------------+
        | Step 2: JSON Flattening (LATERAL FLATTEN MERGE) |
        +------------------------+------------------------+
                                 |
                                 v
        +-------------------------------------------------+
        | Step 3: Silver Transformation, Lookup & Audit   |
        +------------------------+------------------------+
                                 |
                                 v
        +-------------------------------------------------+
        | Step 4: Staging Country Split Procedure (SCD 2) |
        +------------------------+------------------------+
                                 |
                                 v
        +-------------------------------------------------+
        | Step 5: Pre-Promotion Quality Gate Validations  |
        +------------------------+------------------------+
                                 |
                                 v
        +-------------------------------------------------+
        | Step 6: Gold Sync SP & Fact Table MERGE         |
        +-------------------------------------------------+
```

---

## 4. Key Engineering Decisions and Trade-Off Analysis

| Architectural Decision | Design Rationale | Technical Trade-Off and Alternatives Considered |
|---|---|---|
| **Schema-on-Read Landing (All VARCHAR)** | Inbound flat files contain unexpected formats and dropped leading zeros. Landing as strings prevents pipeline halts and enables audit capturing. | Requires explicit type casting in Silver tier, but prevents data loss and ETL crashes. |
| **Dynamic Reference Catalog (`REF_COUNTRY_LOOKUP`)** | Decouples country alias normalization from SQL query code. Supports alias matching and prefix fallback. | Avoids maintaining brittle 50-line `CASE` statements that break whenever new regional variations appear. |
| **SCD Type 2 Migration Tracking** | Preserves historical member residence when moving between countries (`IS_ACTIVE=0`, `END_DATE`), while assigning active status (`IS_ACTIVE=1`) to the current location. | An SCD Type 1 overwrite approach would destroy past residency history, invalidating regional compliance audits. |
| **Isolated Rejection Store (`STG_MEMBERS_REJECTED`)** | Corrupted records are flagged and routed to a dedicated audit table with diagnostic reason codes rather than silently dropped. | Enables operational visibility for customer remediation while preventing bad data from entering Gold marts. |
| **Dynamic Table Provisioning via Stored Procedures** | Scans distinct valid countries and provisions `STG_MEMBERS_<COUNTRY>` and `TABLE_<COUNTRY>` automatically. | Eliminates the need to write and deploy hundreds of physical static DDL files as the airline expands into new markets. |
| **Enriched Analytical Fact Mart (`FACT_MEMBER_REDEMPTIONS`)** | Joins flattened redemption line items with conformed member attributes into a single queryable consumption table. | Pre-joining dimensions at the transaction grain provides O(1) query latency for business dashboards. |

---

## 5. Scalability Blueprint for High-Throughput Processing

To handle sustained high-volume processing across billions of daily events, the platform incorporates specific architectural patterns:

1. Micro-Partition Pruning and Custom Clustering:
   - Tables are clustered based on dominant query access paths: `(BATCH_ID, MEMBER_ID)` on landing tables, `(MEMBER_ID)` on country tables, and `(TXN_DATE, MEMBER_ID)` on the redemption fact table.
   - Eliminates 90%+ of scanned partition data during point lookups and date-range reporting.

2. Idempotent Processing and Batch Scoping:
   - Pipeline stages are scoped by run tokens (`BATCH_ID`).
   - Re-running an aborted batch deletes that batch slice before loading, guaranteeing zero duplicate rows across re-runs.
   - Redemptions use transactional `MERGE` on `TXN_ID` to handle out-of-order and updated transaction payloads.

3. Multi-Cluster Warehouse Scaling:
   - For massive batch windows, `SKYPOINTS_LOYALTY_MEMBERSHIP_WH` is configured with multi-cluster scaling (`MAX_CLUSTER_COUNT = 5`, `AUTO_SUSPEND = 60`) to automatically allocate compute clusters during peak loads and scale down when idle.

4. Continuous Streaming Evolution:
   - The landing stage architecture is directly compatible with serverless **Snowpipe** (`CREATE PIPE ... AUTO_INGEST = TRUE`), allowing continuous micro-batch loading from cloud object stores (S3, GCS, Azure Blob) as files land.
