# NYC Taxi TLC Data Engineering Assessment

End-to-end pipeline: **DBT → Airflow → SQL analytics → PySpark** over the NYC TLC Yellow Taxi 2023 dataset (~38M rows).

---

## Architecture Overview

```
Raw Parquet (S3 / local)
        │
        ▼
  [scripts/load_data.py]          ← Downloads 12 monthly files, loads into DuckDB
        │
        ▼
  DBT Staging (views)
    stg_yellow_trips               ← rename, cast, add trip_duration_minutes
    stg_taxi_zones                 ← seed CSV → clean dimension
        │
        ▼
  DBT Intermediate (ephemeral)
    int_trips_enriched             ← join zones, filter invalid rows
        │
        ▼
  DBT Marts (tables)
    fct_trips                      ← core fact table
    dim_zones                      ← zone dimension
    agg_daily_revenue              ← daily rollup
    agg_zone_performance           ← monthly zone ranking + high-volume flag
        │
        ▼
  Airflow DAG (nyc_taxi_daily_pipeline)
    check_source_freshness
    → run_dbt_staging
    → run_dbt_intermediate
    → run_dbt_marts
    → run_dbt_tests
    → notify_success
        │
        ▼
  SQL Queries (queries/)           ← ad-hoc analytics
  PySpark (spark/)                 ← historical 2009-2023 batch processing
```

### Why these modelling decisions?

- **Staging as views**: No storage cost; always reflects the latest raw data. Staging should never be queried directly by analysts so view performance is fine.
- **Intermediate as ephemeral**: `int_trips_enriched` is a pure transformation step — no one queries it directly. Ephemeral avoids creating a table that would need to be maintained.
- **Marts as tables**: Analysts and dashboards hit these repeatedly. Materialising as tables makes every query fast regardless of warehouse size.
- **Surrogate key via MD5**: The raw dataset has no natural primary key. MD5 on `(pickup_datetime, dropoff_datetime, PULocationID, DOLocationID, fare_amount)` gives a stable, deterministic key. Not perfect (collisions theoretically possible) but good enough for this dataset size.

---

## Setup Instructions

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Download data

```bash
python scripts/load_data.py --output-dir data/ --load-duckdb
```

This downloads all 12 monthly Parquet files (~3-4 GB) and loads them into a local DuckDB file at `/tmp/nyc_taxi.duckdb`.

### 3. Configure DBT

```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
# Edit if needed — default points to DuckDB, no credentials required
```

Download the zone lookup seed:
```bash
curl -o dbt/seeds/taxi_zone_lookup.csv \
  https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

### 4. Run DBT

```bash
cd dbt
dbt deps
dbt seed                          # load taxi_zone_lookup.csv
dbt run                           # build all models
dbt test                          # run all tests
```

To target Snowflake instead of DuckDB:
```bash
export SNOWFLAKE_ACCOUNT=...
export SNOWFLAKE_USER=...
export SNOWFLAKE_PASSWORD=...
dbt run --target snowflake
```

### 5. Run Airflow (local)

```bash
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow webserver -p 8080 &
airflow scheduler &
# Trigger manually:
airflow dags trigger nyc_taxi_daily_pipeline --exec-date 2023-06-01
```

### 6. Run SQL queries

Using DuckDB CLI:
```bash
duckdb /tmp/nyc_taxi.duckdb < queries/q1_top_zones_by_revenue.sql
```

### 7. Run PySpark (bonus)

```bash
spark-submit spark/process_historical.py \
  --input-path "data/yellow_tripdata_20*.parquet" \
  --output-path "output/agg_daily_revenue"
```

---

## Brainstormer Answers

### 1. Monthly vs. annual revenue ranking (`agg_zone_performance`)

**Implemented: monthly ranking** — `RANK() OVER (PARTITION BY year, month ORDER BY total_revenue DESC)`.

An annual rank gives every zone a fixed position for the entire year. That answers "who won 2023" but doesn't help with any operational decision that varies over time. Monthly ranking reveals: which zones are seasonal (JFK spikes in summer), which are consistently dominant (Midtown), and which are fast-rising or declining. A fleet dispatcher making driver-incentive decisions in March cares about March's top zones, not last August's.

### 2. Blue/green deployment for mart models (Airflow `run_dbt_tests` failure)

**The problem**: If `run_dbt_marts` completes and then `run_dbt_tests` fails halfway through, the mart tables are already live and contain bad data. Downstream dashboards are already reading from them.

**Approach — DBT `--defer` + staging schema swap**:

1. Run all mart models into a **staging schema** (e.g. `marts_staging`) rather than `marts`.
2. Run all tests against `marts_staging`.
3. Only if all tests pass, execute an atomic schema swap:
   ```sql
   ALTER SCHEMA marts RENAME TO marts_old;
   ALTER SCHEMA marts_staging RENAME TO marts;
   DROP SCHEMA marts_old CASCADE;
   ```
4. If tests fail, `marts_staging` is dropped and `marts` (the last good version) remains untouched.

Snowflake supports this pattern natively since schema renames are metadata-only operations (near-instant, no data copy). In dbt this can be implemented using a `post-hook` macro or a dedicated Airflow task that calls the swap SQL via a `SnowflakeOperator`.

This is the data engineering equivalent of a blue/green deploy: consumers always read from a schema that has passed tests.

### 3. Query 3 performance on 38M rows

See the comment block in `queries/q3_consecutive_gap_analysis.sql`. Summary: cluster key on `(pickup_date, pickup_location_id)`, result caching for repeat runs, and materialise as a dbt table for production use.

---

## Trade-offs & Shortcuts

| Decision | Reason |
|---|---|
| Used DuckDB as default target | No Snowflake account required to run locally; SQL is compatible with both |
| MD5 surrogate key vs UUID | MD5 is deterministic across runs; UUID would change on re-seed |
| Airflow BashOperator for dbt | Simpler than Cosmos for a self-contained assessment; Cosmos is preferred in production |
| Zone CSV downloaded manually | Could be automated via Airflow sensor; kept simple for clarity |
| PySpark script not cluster-tested | Script is logically correct; actual EMR/Glue deployment would need cluster config tuning |

---

## AI Tools Used

- **Claude (Anthropic)** — used throughout to draft SQL models, review logic, generate the PySpark script, and write this README. All generated code was reviewed and adjusted for correctness (e.g. DuckDB vs Snowflake dialect differences, correct window function semantics for the Brainstormer questions).
- Approach: provided the assessment spec, iterated on each task, reviewed output for correctness rather than accepting it blindly.
