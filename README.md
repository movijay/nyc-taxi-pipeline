# NYC Taxi TLC Data Engineering Assessment

End-to-end pipeline: **DBT -> Airflow -> SQL analytics -> PySpark** over the NYC TLC Yellow Taxi 2023 dataset (~38M rows).

---

## Live Deployment Evidence

> Fully deployed and validated end-to-end: Snowflake + dbt Cloud + Astronomer + GitHub Actions + PySpark on Kaggle.

| Component | Status | Detail |
|---|---|---|
| Snowflake data load | Complete | 38.3M rows, 12 months of Parquet |
| dbt models | 6 models, 29 tests, 0 warnings | stg, intermediate, 4 mart tables |
| Astronomer deployment | HEALTHY | Permanent cloud Airflow deployment |
| GitHub Actions CI/CD | ~11s per deploy | Auto-deploys DAGs on push to main |
| End-to-end Airflow run | 4 min 37 sec | Airflow -> dbt Cloud -> Snowflake |
| Analytics queries | Q1/Q2/Q3 proven | Real results from 35.5M row mart |
| PySpark bonus | 2009-2023, 362 files | Kaggle notebook, full pipeline |

![Astronomer Deployment](docs/screenshots/astronomer_deployment.png)
*Astronomer -- HEALTHY, 3 successful deploys*

![GitHub Actions Workflow List](docs/screenshots/github_actions_list.png)
*GitHub Actions CI/CD -- auto-deploy on every push to main*

![GitHub Actions Detail](docs/screenshots/github_actions_detail.png)
*Individual workflow run -- completes in ~11 seconds*

![Airflow DAG Run](docs/screenshots/airflow_dag_run.png)
*Airflow run_dbt_pipeline -- 4 min 37 sec, all tasks succeeded*

---

## Architecture Overview

```
Raw Parquet (Snowflake Internal Stage)
        |
        v
  [SnowSQL PUT + COPY INTO]       <- 12 monthly Parquet files, USE_LOGICAL_TYPE=TRUE
        |
        v
  DBT Staging (views)
    stg_yellow_trips               <- rename, cast, QUALIFY ROW_NUMBER() dedup on trip_id
    stg_taxi_zones                 <- seed CSV -> clean dimension
        |
        v
  DBT Intermediate (ephemeral)
    int_trips_enriched             <- join zones, filter invalid rows
        |
        v
  DBT Marts (tables)
    fct_trips                      <- core fact table (35.5M rows)
    dim_zones                      <- zone dimension (265 rows)
    agg_daily_revenue              <- daily rollup (365 rows)
    agg_zone_performance           <- monthly zone ranking (3,040 rows)
        |
        v
  Airflow DAG (run_dbt_pipeline) on Astronomer
    -> Calls dbt Cloud REST API    <- PythonOperator, polls every 30s
        |
        v
  GitHub Actions CI/CD             <- astro_deploy.yml, triggers on push to main
        |
        v
  SQL Queries (queries/)           <- Snowflake Snowsight analytics
  PySpark (spark/)                 <- Historical 2009-2023 batch (bonus)
```

### Why these modelling decisions?

- **Staging as views**: No storage cost; always reflects latest raw data. Analysts never query staging directly so view latency is acceptable.
- **Intermediate as ephemeral**: `int_trips_enriched` is a pure transformation -- no one queries it directly. Ephemeral avoids an unnecessary table and inlines SQL at compile time.
- **Marts as tables**: Hit repeatedly by analysts. Materialising as tables ensures sub-second query times regardless of warehouse size.
- **Surrogate key via MD5**: No natural primary key in raw data. MD5 over `(pickup_datetime, dropoff_datetime, PULocationID, DOLocationID, fare_amount, tip_amount, total_amount, passenger_count, trip_distance)` gives a stable, deterministic key across re-runs.
- **QUALIFY ROW_NUMBER() deduplication**: Raw Parquet files contain exact duplicate rows. Deduplication at staging resolved 3 dbt uniqueness warnings -- achieving 29 pass / 0 warn.

---

## Setup Instructions

> **Note:** Original assessment described DuckDB + local Airflow. This implementation uses **Snowflake + dbt Cloud + Astronomer + GitHub Actions**.

### 1. Clone the repository

```bash
git clone https://github.com/<your-org>/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline
```

### 2. Load data into Snowflake

```sql
CREATE OR REPLACE DATABASE nyc_taxi;
CREATE OR REPLACE SCHEMA nyc_taxi.raw;
CREATE OR REPLACE STAGE nyc_taxi.raw.tlc_stage;

PUT file:///path/to/yellow_tripdata_2023-01.parquet @nyc_taxi.raw.tlc_stage;
-- repeat for all 12 months

COPY INTO nyc_taxi.raw.yellow_trips
FROM @nyc_taxi.raw.tlc_stage
FILE_FORMAT = (TYPE = PARQUET USE_LOGICAL_TYPE = TRUE)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

```bash
curl -o dbt/seeds/taxi_zone_lookup.csv \
  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

### 3. Configure dbt Cloud

1. Create a dbt Cloud account and connect your GitHub repo.
2. Set **Project subdirectory** to `dbt/`.
3. Create a Snowflake connection with your credentials.
4. The `generate_schema_name.sql` macro is already in `dbt/macros/`.
5. Create a **dbt Cloud Job**: `dbt deps` -> `dbt seed` -> `dbt run` -> `dbt test`
6. Note the **Job ID** and generate an **API token**.

### 4. Deploy to Astronomer

1. Install: `curl -sSL install.astronomer.io | sudo bash -s`
2. Login: `astro login`
3. Astronomer UI: create a Deployment, enable **DAG-only deploys**.
4. Set Airflow Variables: `dbt_cloud_api_token`, `dbt_cloud_account_id`, `dbt_cloud_job_id`
5. Deploy: `astro deploy --dags`

### 5. Configure GitHub Actions

1. Astronomer -> Workspace Settings -> API Tokens -> create **Workspace Owner** token.
2. Add as GitHub secret: **`ASTRO_API_TOKEN`**
3. `.github/workflows/astro_deploy.yml` is already configured.

![GitHub Actions Detail](docs/screenshots/github_actions_detail.png)
*Successful GitHub Actions run -- ~11 seconds*

### 6. Trigger the pipeline

Astronomer UI -> Deployment -> Open Airflow -> trigger `run_dbt_pipeline`. Completes in ~4-5 minutes.

### 7. Run SQL analytics queries

All queries in `queries/` -- run in Snowflake Snowsight.

### 8. Run PySpark -- Historical Processing 2009-2023 (bonus)

**Kaggle Notebook** (recommended -- no local setup):
Open `spark/kaggle_historical_demo.ipynb` at [kaggle.com/code](https://kaggle.com/code) and run all cells.

**Local run**:
```bash
pip install pyspark
spark-submit spark/process_historical.py \
  --input-path "data/yellow_tripdata_20*.parquet" \
  --output-path "output/agg_daily_revenue"
```

**AWS production:**
- EMR: `spark-submit --deploy-mode cluster --master yarn spark/process_historical.py`
- Glue: Replace `SparkSession` with `GlueContext`; output via `write_dynamic_frame_from_options` with `partitionKeys=[year, month]`

**Verified Kaggle run (actual output):**

```
Raw rows:        150,000
After filtering:  20,148  (same rules as dbt models)

Trips per year:
2009: 1,367  | 2010: 1,381  | 2011: 1,276  | 2012: 1,347  | 2013: 1,343
2014: 1,431  | 2015: 1,349  | 2016: 1,280  | 2017: 1,340  | 2018: 1,361
2019: 1,354  | 2020: 1,361  | 2021: 1,332  | 2022: 1,319  | 2023: 1,307

Parquet files written: 362 (partitioned by year/month)
```

![PySpark Kaggle Run](docs/screenshots/pyspark_kaggle_run.png)
*PySpark Kaggle run -- 150K rows, all 15 years 2009-2023, 362 partitioned Parquet files*

Sample output: `spark/sample_output/agg_daily_revenue_sample.csv`

---

## dbt Run & Test Results

![dbt Run Success](docs/screenshots/dbt_run_success.png)
*dbt run -- 6 models succeeded (int_trips_enriched is ephemeral -- no-op is expected)*

![dbt Test Results](docs/screenshots/dbt_test_results.png)
*dbt test -- 29 tests passing, 0 warnings, 0 errors*

![dbt Lineage](docs/screenshots/dbt_lineage.png)
*dbt lineage DAG -- source -> staging -> intermediate -> marts*

---

## Snowflake Validation

![Snowflake Row Counts](docs/screenshots/snowflake_row_count.png)
*Snowflake row counts -- 38.3M raw, 35.5M valid trips in fct_trips*

![Snowflake Schemas](docs/screenshots/show_schemas.png)
*Snowflake schemas -- RAW, RAW_STAGING, RAW_MARTS all populated*

---

## Analytics Query Results

![Q1 Results](docs/screenshots/q1_results.png)
*Q1: Top 10 pickup zones by revenue per month -- 29ms*

![Q2 Results](docs/screenshots/q2_results.png)
*Q2: Hour-of-day demand pattern with 3-hour rolling average -- 894ms, 24 rows*

![Q3 Results](docs/screenshots/q3_results.png)
*Q3: Consecutive trip gap analysis (LAG over 35.5M rows) -- 2.5 seconds, 65,735 rows*

---

## Brainstormer Answers

### 1. Monthly vs. annual revenue ranking (`agg_zone_performance`)

**Implemented: monthly ranking** -- `RANK() OVER (PARTITION BY year, month ORDER BY total_revenue DESC)`.

An annual rank gives every zone a fixed position for the entire year -- useful for an annual report, but useless for operational decisions. Monthly ranking reveals which zones are seasonal (JFK spikes every summer), which are consistently dominant (Midtown Manhattan holds top-3 every month), and which are fast-rising or declining. A fleet dispatcher making driver-incentive decisions in March cares about March's top zones, not last August's. The mart produces 3,040 rows (265 zones x 12 months) -- low enough for fast dashboard queries without further aggregation.

### 2. Blue/green deployment for mart models (Airflow `run_dbt_tests` failure)

**The problem**: `run_dbt_marts` writes directly to the live `marts` schema. If `run_dbt_tests` fails halfway through, analysts are already reading bad data.

**Approach -- staging schema swap (Snowflake-native):**

1. Run all mart models into `marts_staging` (not `marts`).
2. Run all dbt tests against `marts_staging`.
3. Only if all tests pass, execute an atomic schema rename:

```sql
ALTER SCHEMA marts RENAME TO marts_old;
ALTER SCHEMA marts_staging RENAME TO marts;
DROP SCHEMA marts_old CASCADE;
```

4. If tests fail, drop `marts_staging` -- the live `marts` schema (last good version) is untouched.

Snowflake schema renames are metadata-only: near-instant, zero data copy, no query interruption. Implemented as a dbt `post-hook` macro or a dedicated `SnowflakeOperator` in Airflow that fires only after all tests pass. This is the data engineering equivalent of a blue/green deploy.

### 3. Query 3 performance on 38M rows

**Query**: For each `(pickup_date, pickup_location_id)` pair, find the maximum gap in minutes between consecutive trips using `LAG()` ordered by `pickup_datetime` within each zone.

**Actual measured performance** (run against the real 35.5M-row `fct_trips` in Snowflake Snowsight):
- Result set: **65,735 rows**
- Runtime: **2.5 seconds** on Snowflake X-Small warehouse
- Key insight: outer-borough zones see gaps up to 18 hours on low-demand days; Midtown zones never exceed 3-minute gaps during peak hours

**Production optimisations** (documented in `queries/q3_consecutive_gap_analysis.sql`):

1. **Cluster key**: `ALTER TABLE fct_trips CLUSTER BY (pickup_date, pickup_location_id)` co-locates rows by the LAG() partition, reducing micro-partition scans from O(35.5M) to O(rows per zone-day slice).
2. **Result caching**: Snowflake caches exact query results for 24 hours at zero compute cost -- repeat loads return instantly.
3. **Materialise as dbt table**: Run as `materialized='table'` so analysts query pre-computed results rather than re-running the full LAG() scan.
4. **Warehouse sizing**: 2.5s on X-Small is acceptable for daily batch use. For sub-second interactive queries, size up to Small or persist results as a table.

---

## Trade-offs & Shortcuts

| Decision | Reason |
|---|---|
| Snowflake instead of DuckDB | Production-grade; scales to 38M+ rows; SQL dialect consistent with all analytics queries; compatible with dbt Cloud and Astronomer |
| Snowflake internal stage instead of S3 | No external bucket required; SnowSQL `PUT` handles local Parquet upload; `USE_LOGICAL_TYPE=TRUE` preserves timestamp precision |
| dbt Cloud instead of local dbt CLI | Managed job history, lineage DAG UI, CI/CD hooks, and REST API for Airflow polling out of the box |
| Astronomer instead of local Airflow | Managed Airflow with persistent deployment; DAG-only deploys via GitHub Actions keep infrastructure fully as code |
| Airflow calls dbt Cloud REST API (PythonOperator) | Simpler than dbt Cosmos for this scope; decouples orchestration from transformation; Cosmos preferred in production for task-level observability |
| GitHub Actions CI/CD (`ASTRO_API_TOKEN`) | Free, native to GitHub; 8-11 second deploy cycle; Workspace Owner token is the correct Astronomer auth scope |
| MD5 surrogate key | Deterministic across re-runs; UUID would generate a new key on every seed and break incremental joins |
| QUALIFY ROW_NUMBER() deduplication | Eliminated 3 dbt uniqueness warnings from exact duplicate rows in raw Parquet source files |
| PySpark validated on Kaggle | No AWS cluster cost; full 2009-2023 pipeline proven end-to-end (150K rows, 362 Parquet files, broadcast zone join, F.nullif tip rate); production EMR/Glue deployment notes in script |
| Zone CSV loaded via dbt seed | Reproducible and version-controlled; no manual download step in CI |

---

## AI Tools Used

- **Claude (Anthropic)** -- used throughout to draft SQL models, review logic, debug dbt uniqueness warnings (`QUALIFY ROW_NUMBER()` fix), write the Airflow DAG with dbt Cloud REST API polling, configure GitHub Actions CI/CD (`ASTRO_API_TOKEN`), generate the PySpark script and Kaggle notebook (Java auto-detection, broadcast zone join, `F.nullif`), and write this README. All generated code was reviewed against real system outputs (Snowflake runtimes, dbt test results, Airflow run logs, Kaggle PySpark output).
- Approach: provided the assessment spec, iterated on each component, validated every output against live system results rather than accepting generated code blindly.
