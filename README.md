# Snowflake Wine Reviews Analytics Platform

End-to-end data engineering and analytics project built in Snowflake using the Kaggle Wine Reviews dataset (`winemag-data-130k-v2.csv`).

The project implements a medallion architecture (`BRONZE`, `SILVER`, `GOLD`) with:

- Batch + incremental ingestion
- Star-schema modeling
- Gold-layer business aggregates
- Snowflake Cortex AI SQL enrichment
- Cortex Search (semantic search)
- Cortex Analyst (natural language BI via semantic view)
- Streamlit dashboards in Snowflake (described in project scope)

## Project Files

- `Ingestion_and_Schemas.sql`
  - Database/schema setup
  - Bronze ingest objects (file format, stage, raw table, copy)
  - Silver star schema + ETL
  - Gold reporting tables
  - Stream + stored procedures for incremental processing
  - Snowpipe (`WINEMAG_PIPE`) for auto-ingest

- `AI_SQL.sql`
  - AI SQL enrichment on Bronze (`SUMMARIZE`, `SENTIMENT`)
  - Cortex Search service (`WINEMAG_DESCRIPTION_SEARCH`)
  - Cortex Analyst semantic view (`WINE_ANALYTICS_SV`)
  - Includes some repeated pipeline objects/logic for end-to-end execution

- `sample_wine_reviews_50.csv`
  - Small subset of `winemag-data-130k-v2.csv` data
  - Contains 50 rows of data and all columns used for analysis

- `Database design ppt.pdf`
  - Design overview and presentation material

## Architecture (Medallion)

- `BRONZE` (Raw)
  - Stores raw ingested wine review data (`WINEMAG_BRONZE_RAW`)
  - Immutable landing layer / source of truth

- `SILVER` (Conformed)
  - Cleansed, deduplicated, query-ready star schema
  - Dimensions:
    - `DIM_COUNTRY`
    - `DIM_REGION`
    - `DIM_TASTER`
    - `DIM_WINE`
  - Fact:
    - `FACT_WINE_REVIEW`

- `GOLD` (Business Aggregates)
  - Analytics-ready tables for BI/reporting
  - `PRICE_BAND_METRICS`
  - `WINERY_METRICS`
  - `TOP_WINES`

## Data Pipeline Flow

1. CSV is uploaded to Snowflake stage `BRONZE.CSV_STAGE`
2. Snowpipe (`BRONZE.WINEMAG_PIPE`) loads data into `BRONZE.WINEMAG_BRONZE_RAW`
3. Stream (`BRONZE.WINEMAG_BRONZE_STREAM`) captures inserts
4. `BRONZE.SP_LOAD_SILVER_TABLES()` merges/inserts incremental rows into Silver
5. `SILVER.SP_LOAD_GOLD_TABLES()` rebuilds Gold aggregates
6. AI enrichments and Cortex services enable semantic search + NL analytics
7. Streamlit dashboards consume Gold/Silver outputs for interactive analysis

## Key Features

### 1. Ingestion & Automation

- `CSV_FORMAT` and `CSV_STAGE` for raw file loading
- `COPY INTO` for initial batch ingestion
- `AUTO_INGEST = TRUE` Snowpipe for near real-time file ingestion
- Bronze stream for change tracking
- Incremental stored procedures for downstream propagation

### 2. Dimensional Modeling (Silver)

- Star schema optimized for analytics
- Surrogate keys and relational integrity across dimensions/fact
- Fact table stores core measures (`price`, `points`) and review text/title

### 3. Business Analytics (Gold)

- `PRICE_BAND_METRICS`
  - Aggregates by country and price band
  - Tracks average points, average price, review counts

- `WINERY_METRICS`
  - Winery performance by winery/country/province
  - Includes ranking (`RANK()`) within country

- `TOP_WINES`
  - High-scoring wines (90+ points)
  - Ranked by country/variety/price band using `ROW_NUMBER()`

### 4. AI & Advanced Analytics (Snowflake Cortex)

- AI enrichment on Bronze:
  - `SNOWFLAKE.CORTEX.SUMMARIZE(description)`
  - `SNOWFLAKE.CORTEX.SENTIMENT(description)`
- Semantic search service:
  - `WINEMAG_DESCRIPTION_SEARCH`
  - Supports natural-language queries + structured filters (country/price/points)
- Natural language BI:
  - Semantic view `SILVER.WINE_ANALYTICS_SV`
  - Enables analyst-style NL-to-SQL workflows on the Silver model

## How to Run

### 1) Update environment-specific names

The scripts currently use team-specific names such as:

- `ROLE_TEAM_ROCKSTARS`
- `DB_TEAM_ROCKSTARS`
- `ANIMAL_TASK_WH`

Replace these with your assigned Snowflake role, database, and warehouse if needed.

### 2) Upload dataset to Snowflake stage

- Upload `winemag-data-130k-v2.csv` to `DB_TEAM_ROCKSTARS.BRONZE.CSV_STAGE`
- Ensure stage and file format are created before loading

### 3) Run core pipeline script

Execute `Ingestion_and_Schemas.sql` to:

- Create schemas/tables
- Load Bronze
- Build Silver and Gold
- Create stream + stored procedures
- Create Snowpipe

### 4) Run AI/Cortex script

Execute `AI_SQL.sql` to:

- Add AI-generated summary/sentiment columns to Bronze
- Populate AI enrichment values
- Create Cortex Search service and example searches
- Create semantic view for Cortex Analyst

## Notes / Prerequisites

- Snowpipe `AUTO_INGEST` requires proper cloud event notification integration in your Snowflake environment.
- Cortex features require account/region support and appropriate privileges.
- Streamlit dashboards are part of the project scope/design, but dashboard app code is not included in this repository.
- Some logic appears in both SQL files (pipeline objects are repeated in `AI_SQL.sql`).

## Example Business Questions Supported

- Which wineries consistently produce the highest-rated wines, and at what price points?
- How does wine price relate to review quality across countries?
- What are the top wines by country, variety, and price band?
- Which wines match a semantic query such as "fruity red wine with cherry notes"?

## Technologies Used

- Snowflake (SQL, Warehousing, Snowpipe, Streams, Stored Procedures)
- Snowflake Cortex (AI SQL, Cortex Search, Cortex Analyst/Semantic View)
- Streamlit in Snowflake (dashboard layer)
- Kaggle Wine Reviews dataset

## Outcomes

- Production-style modern data platform in Snowflake
- Automated ingestion + incremental ELT pipeline
- Query-efficient star schema and business aggregates
- AI-powered semantic search and natural-language analytics
- Business-ready datasets for dashboarding and decision support
