# SkyPoints Global Rewards - Scalable Data Engineering Platform

A multi-tiered, resilient and dynamic data warehouse implementation on Snowflake engineered to ingest, cleanse, standardize, and segment high-volume customer loyalty data and partner airline redemption activity across international markets.

---

## Business Scenario and Objectives

SkyPoints operates an international airline rewards network that enables members to accumulate miles, maintain status tiers, and redeem rewards across multiple partner airlines worldwide.

Historically, customer demographics and engagement metrics were centralized in a single repository. To optimize localized analytical reporting, regulatory compliance, and query performance, this platform transitions customer records into dedicated regional tables while establishing an enriched fact layer for partner redemption events.

The ingestion pipeline handles two distinct data streams:
1. Daily Member Roster: Delimited flat files with regional variations in date formats, optional fields, and inconsistent geographic naming.
2. Partner Redemption Stream: Semi-structured JSON payloads detailing mileage transactions, partner identities, and fulfillment statuses.

---

## End-to-End Pipeline Architecture

The platform follows a three-stage Medallion pattern designed for fault-tolerant batch and micro-batch processing:

```
                            Inbound Source Feeds
                     (S3 / Blob / Snowflake Stage)
                     +---------------------------+
                     | - Pipe-Delimited Profiles |
                     | - Semi-Structured JSON    |
                     +-------------+-------------+
                                   |
                                   v
+-------------------------------------------------------------------------+
| RAW (Bronze Layer) - Resilient Ingestion                                |
| - RAW_MEMBERS: Loose typing (all VARCHAR) prevents file load aborts     |
| - RAW_REDEMPTIONS: Native VARIANT column preserves original JSON        |
+----------------------------------+--------------------------------------+
                                   |
                                   v
+-------------------------------------------------------------------------+
| STG (Silver Layer) - Transformation, Quality and Standardization        |
| - Standardizes dates (DDMMYYYY, YYYY-MM-DD, integer formats)            |
| - Resolves country variations via dynamic lookup (REF_COUNTRY_LOOKUP)   |
| - Calculates derived attributes: Member Age and Inactivity Flag         |
| - Flattens nested JSON transactions into relational rows                |
| - Applies data quality rules: isolates corrupt rows to rejections table |
| - SP_CREATE_AND_LOAD_COUNTRY_TABLES: Manages SCD Type 2 country moves   |
+-------------------+---------------------------------+-------------------+
                    |                                 |
           [Valid Records]                    [Invalid Records]
                    |                                 |
                    v                                 v
+---------------------------------------+  +------------------------------+
| PROD (Gold Layer) - Consumption Marts |  | Rejection Audit Store (DLQ)  |
| - Regional Tables (TABLE_<COUNTRY>):  |  | - STG_MEMBERS_REJECTED:      |
|   TABLE_IND, TABLE_USA, TABLE_AUS,    |  |   Captures failed records    |
|   TABLE_CAN, TABLE_PHL, etc.          |  |   with explicit reason codes |
| - FACT_MEMBER_REDEMPTIONS:            |  |   (missing IDs, future dates,|
|   Transaction-grain fact table joined |  |   enrollment after flight)   |
|   with member attributes              |  +------------------------------+
+---------------------------------------+
```

---

## Core Architectural Principle:  

A foundational design requirement of this platform is that **no table names, country codes, column layouts, or batch runs are ever hardcoded**. The system is completely autonomous and dynamically adapts to inbound data at runtime:

1. **Dynamic Regional Table Provisioning**:
   - The stored procedures `STG.SP_CREATE_AND_LOAD_COUNTRY_TABLES()` and `PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES()` scan inbound data for distinct countries dynamically.
   - When a new territory appears (e.g., Canada `CAN`, Philippines `PHL`, Sweden `SWE`, Germany `DEU`), Snowflake **automatically provisions physical tables** (`STG.STG_MEMBERS_<COUNTRY>` and `PROD.TABLE_<COUNTRY>`) using dynamic DDL (`EXECUTE IMMEDIATE`), complete with clustering keys and comments. **Zero code changes or manual DDL deployment are required.**
2. **Dynamic File Structure & Column Absorption**:
   - The ingestion layer automatically adjusts for column layout shifts between 11-column legacy feeds (omitting postal codes) and 12-column current feeds:
     `CASE WHEN $12 IS NOT NULL THEN $10 ELSE $12 END AS POST_CODE`
3. **Dynamic Multi-Branch Date Inference**:
   - Inbound dates are parsed without pre-configured format masks. The pipeline dynamically tests and extracts `YYYY-MM-DD`, `YYYYMMDD`, `MMDDYYYY`, `DDMMYYYY`, and dropped leading zero formats (`3051985` $\rightarrow$ `03051985`).
4. **Dynamic Geographic Alias Resolution (`STG.REF_COUNTRY_LOOKUP`)**:
   - Inbound variations (`India`, `Indi`, `IN`, `Bharat`, `Oz`, `AUST`, `CDA`, `America`) are resolved at runtime via the reference catalog into uniform ISO 3166-1 alpha-3 codes.
5. **Dynamic Schema Evolution**:
   - Tables across all schemas utilize idempotent schema evolution (`ALTER TABLE ... ADD COLUMN IF NOT EXISTS`) so subsequent schema modifications automatically propagate without dropping tables or losing historical data.
6. **Dynamic Batch Scoping & Idempotency**:
   - Every execution dynamically stamps a timestamp batch token (`$BATCH_ID = TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS')`). Re-running batches or processing late-arriving files updates state in-place with zero row duplication.

---

## AI-Assisted Engineering & Model Stack

This enterprise data platform was designed, engineered, and hardened using advanced generative AI pair programming models, specifically **Gemini 3.8** and **Claude Sonnet 4.7**:

| AI Model  | Primary Project Role & Where Used | Specific Technical Contributions |
| :--- | :--- | :--- |
| **Google Gemini 3.8** | **Cloud Data Warehouse Architecture & Snowflake Optimization** | - **Snowflake Runtime Engine Optimization**: Identified subtle SQL compilation pitfalls, including `CONCAT_WS` returning `NULL` for optional string arrays (resolved via `ARRAY_TO_STRING(ARRAY_CONSTRUCT_COMPACT(...), '\|')`) and Snowflake's restrictions on nested CTEs in `MERGE` queries.<br>- **Micro-Partition Pruning Strategy**: Designed optimal clustering key topologies (`MEMBER_ID` on country tables, `(TXN_DATE, MEMBER_ID)` on the redemption fact) for high-throughput scaling.<br>- **Bronze Ingestion Resilience**: Structured schema-on-read patterns to prevent data type landing failures on dirty flat files. |
| **Anthropic Claude Sonnet 4.7** | **Procedural Logic, SCD-2 Mechanics & Complex Transformation** | - **Dynamic Stored Procedure Generation**: **Temporal SCD Type 2 Window Logic**: Engineered non-trivial analytical windowing (`LEAD()`, `ROW_NUMBER()`) ordered by business effective dates (`COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS)`) to ensure historical accuracy during customer country migration.<br>- **Multi-Variant Date Parser**: Developed the comprehensive regular expression and substring parser handling dropped leading zeros and cross-century year disambiguation.<br>- **Quality Gate & Dead Letter Queue**: Implemented the rejection categorization engine isolating corrupt records into `STG_MEMBERS_REJECTED`. |
| **Collaborative Workflow** | **End-to-End Validation & Documentation** | - **Synthetic Edge-Case Data Generation**: Created test feeds (`members_batch2.csv`, `members_invalid_dates.csv`, `members_late_arriving.csv`, `redemptions_batch2.json`) simulating complex data defects.<br> |

---

## Technical Solutions to Source Data Complexities

### 1. Dynamic Geographic Normalization
Member records frequently arrive with non-standard geographic names, varying abbreviations, or colloquial spellings (such as `India`, `Ind`, `Indi`, `IN` for India, or `Aust`, `AU` for Australia).

Instead of maintaining brittle hardcoded conditionals, the solution uses a dedicated reference table `STG.REF_COUNTRY_LOOKUP` combined with a multi-level resolution strategy:
- Level 1 (Exact Alias Lookup): Matches pre-registered abbreviations and aliases directly against standardized ISO 3166-1 alpha-3 codes (`IND`, `AUS`, `USA`, `CAN`, `PHL`, `GBR`, etc.).
- Level 2 (Prefix Matching): If an unseen variant arrives, a prefix comparison evaluates against canonical country names (for example, `Canad` maps to `Canada` -> `CAN`).
- Level 3 (Validation Fallback): Inputs that cannot be reliably matched evaluate to `NULL`, triggering validation rules that quarantine the record for review.

This structure guarantees that downstream procedures dynamically generate consistent, non-redundant tables (`PROD.TABLE_IND` rather than separate `TABLE_INDIA` and `TABLE_IND`).

### 2. Handling Missing Leading Digits in Dates
Birth dates are received as numeric text, but dropped leading zeros frequently produce 7-digit strings (for example, `3051985` representing March 5, 1985).

The staging logic checks the character length of incoming strings and prepends leading zeros before date parsing (`CASE WHEN LENGTH(TRIM(DOB)) = 7 THEN TRY_TO_DATE('0' || TRIM(DOB), 'DDMMYYYY') END`), eliminating erroneous rejections of valid historical records while properly intercepting truly malformed values.

### 3. Cross-Region Member Relocation (SCD Type 2)
When an existing member updates their country of residence, historical reporting must preserve past residency while assigning current membership to the new location.

The pipeline applies analytical window functions (`ROW_NUMBER()` and `LEAD()`) ordered by business effective dates:
- Prior residency records are maintained with `IS_ACTIVE = 0` and an `END_DATE` representing the transition date.
- The latest active profile receives `IS_ACTIVE = 1` and `END_DATE = NULL`.
- Regional tables only maintain member rows relevant to their jurisdiction, preventing duplicate active statuses across territories.

### 4. Derived Business Metrics
During Silver tier staging, records are augmented with operational indicators:
- `AGE`: Calculated dynamically by evaluating `DOB` against `CURRENT_DATE()`.
- `STALE_MEMBER`: Flags members with no flight activity within the past 90 days (`Y`), members with flights inside the 90-day window (`N`), and members without recorded flights (`U`).

### 5. Multi-Level Redemption Fact Model
Redemption events arrive asynchronously from partner systems in nested JSON. In the staging layer, `LATERAL FLATTEN` extracts transaction arrays into individual relational rows.

In the Gold layer, `PROD.FACT_MEMBER_REDEMPTIONS` merges these transactions with the conformed member profile, producing an analytical grain containing transaction metadata, redemption volume, partner name, member tier, and geographic attributes.

---

## Repository Structure

```text
de_assign-main/
|-- README.md                          # Platform documentation and technical guide
|-- master_pipeline.sql                # Unified sequence runner and Snowflake Task DAG blueprint
|-- Results_2026-10-21-0957.csv        # Sample output from pipeline verification runs
|-- DE_Assessment_file.pdf             # Original requirements specification
|
|-- 01_raw/
|   `-- raw_ddl.sql                    # Infrastructure DDL: database, warehouse, schemas, raw tables
|
|-- 02_staging/
|   |-- 01_stg_ddl.sql                 # Staging DDL: STG_MEMBERS, audit table, REF_COUNTRY_LOOKUP
|   |-- 02_stg_transform_load.sql      # Silver logic: transformation, date handling, country mapping
|   |-- 03_dynamic_country_split.sql   # Stored procedure for regional table provisioning (SCD Type 2)
|   `-- 04_json_redemptions.sql        # JSON flattening and member profile join queries
|
|-- 03_target/
|   `-- target_tables.sql              # Gold marts: PROD.TABLE_<COUNTRY> and FACT_MEMBER_REDEMPTIONS
|
|-- 05_validation/
|   `-- validations.sql                # Quality checks: mandatory constraints, uniqueness, temporal logic
|
`-- 06_pipeline/                       # Pipeline automation assets
```

---

## Operating Instructions

### Method 1: Single-Script Execution (Recommended)
The file `master_pipeline.sql` contains the complete operational sequence:
1. Open `master_pipeline.sql` inside a Snowflake Worksheet.
2. Run the script from top to bottom. It establishes the active batch identifier (`$BATCH_ID`), ingests bronze data, cleanses records, runs dynamic territory provisioning, updates the fact mart, and outputs execution metrics.

### Method 2: Modular Execution by Layer

1. Infrastructure and Bronze Ingestion:
   Run `01_raw/raw_ddl.sql` to initialize the database, warehouse, storage stages, and landing tables.

2. Staging Infrastructure and Seeds:
   Run `02_staging/01_stg_ddl.sql` to create staging tables and initialize the geographic lookup catalog.

3. Partner Transaction Processing:
   Run `02_staging/04_json_redemptions.sql` to flatten and load redemption transactions into `STG_REDEMPTIONS`.

4. Member Standardization and Cleansing:
   Run `02_staging/02_stg_transform_load.sql` to process member profiles, derive calculated fields, and route corrupt rows to `STG_MEMBERS_REJECTED`.

5. Regional Territory Provisioning:
   Run `02_staging/03_dynamic_country_split.sql` to execute `SP_CREATE_AND_LOAD_COUNTRY_TABLES()`, creating regional Silver tables.

6. Data Quality Assurance:
   Run `05_validation/validations.sql` to inspect validation results and ensure zero critical defects before production usage.

7. Target Mart Synchronization:
   Run `03_target/target_tables.sql` to refresh `PROD.TABLE_<COUNTRY>` and update `PROD.FACT_MEMBER_REDEMPTIONS`.

---

## High-Throughput Production Considerations

To handle sustained high-volume feeds across billions of daily events, the platform incorporates specific architectural patterns:

1. Pruning and Clustering Keys:
   Tables are clustered on query access patterns (`MEMBER_ID` on country tables, `(TXN_DATE, MEMBER_ID)` on the redemption fact). This maximizes micro-partition pruning and prevents expensive full-table scans.

2. Idempotent Batch Runs:
   Every load operation is scoped by batch tokens and merge keys. Re-running a batch automatically overwrites or reconciles data without creating duplicate records.

3. Automated Production Scheduling:
   Part B of `master_pipeline.sql` provides the configuration for a native Snowflake Task DAG, linking ingestion, transformation, dynamic routing, and mart synchronization into an automated, dependency-aware schedule.
