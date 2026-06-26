"""
nyc_taxi_daily_pipeline
-----------------------
Airflow DAG that orchestrates the NYC Taxi DBT pipeline daily at 02:00 UTC.

Credentials:
  - All dbt profile credentials are injected via environment variables or
    Airflow Connections — nothing is hardcoded here.
  - Set AIRFLOW_CONN_DBT_SNOWFLAKE (or equivalent) in your Airflow environment.

Backfill:
  - Tasks that interact with dated source files use {{ ds }} / data_interval_start
    so backfill runs process the correct partition automatically.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Config — override via Airflow Variables or env vars in production
# ---------------------------------------------------------------------------
DBT_PROJECT_DIR = Path("/opt/airflow/dbt")
DBT_PROFILES_DIR = Path("/opt/airflow/dbt")
DBT_TARGET = "dev"                        # override to 'snowflake' in prod
SOURCE_DATA_PATH = "/opt/airflow/data"    # local path where parquet files live

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@yourcompany.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DBT_BASE_CMD = (
    f"dbt --no-write-json "
    f"--project-dir {DBT_PROJECT_DIR} "
    f"--profiles-dir {DBT_PROFILES_DIR} "
    f"--target {DBT_TARGET}"
)

# ---------------------------------------------------------------------------
# Source freshness check
# ---------------------------------------------------------------------------
def check_source_freshness(data_interval_start, **kwargs):
    """
    Validates that a source Parquet file exists for the execution date.
    Raises FileNotFoundError if missing — this prevents the DAG from proceeding
    on missing upstream data rather than silently running on stale data.

    For the 2023 monthly dataset, this checks that the monthly file for the
    execution month exists. In a streaming / daily-file setup, swap to a daily
    file pattern like yellow_tripdata_YYYY-MM-DD.parquet.
    """
    ds = data_interval_start
    expected_file = (
        Path(SOURCE_DATA_PATH)
        / f"yellow_tripdata_{ds.year}-{ds.month:02d}.parquet"
    )
    if not expected_file.exists():
        raise FileNotFoundError(
            f"Source file not found: {expected_file}. "
            f"Pipeline aborted — no data to process for {ds.date()}."
        )


def notify_success(data_interval_start, **kwargs):
    """
    Pulls trip count and revenue for the execution date from the mart layer
    and logs a summary. In production, replace the print with a Slack/PagerDuty
    notification via an Airflow hook.
    """
    import duckdb  # or use SnowflakeHook for Snowflake target

    ds_date = data_interval_start.date()
    con = duckdb.connect("/tmp/nyc_taxi.duckdb", read_only=True)
    result = con.execute(
        """
        select total_trips, total_fare
        from marts.agg_daily_revenue
        where trip_date = ?
        """,
        [str(ds_date)],
    ).fetchone()
    con.close()

    if result:
        trips, revenue = result
        print(
            f"[SUCCESS] {ds_date} — "
            f"{trips:,} trips | ${revenue:,.2f} total fare"
        )
    else:
        print(f"[SUCCESS] {ds_date} — no data rows found in mart (new day?).")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_daily_pipeline",
    description="NYC Taxi TLC: ingest → dbt staging → intermediate → marts → tests → notify",
    schedule_interval="0 2 * * *",          # 02:00 UTC daily
    start_date=datetime(2023, 1, 1),
    catchup=True,                            # enable backfill
    max_active_runs=3,
    default_args=DEFAULT_ARGS,
    tags=["nyc-taxi", "dbt", "data-engineering"],
) as dag:

    # 1. Validate source file exists for this execution date
    t_check_freshness = PythonOperator(
        task_id="check_source_freshness",
        python_callable=check_source_freshness,
    )

    # 2. Run dbt staging models
    t_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"{DBT_BASE_CMD} run --select staging",
    )

    # 3. Run dbt intermediate models
    t_dbt_intermediate = BashOperator(
        task_id="run_dbt_intermediate",
        bash_command=f"{DBT_BASE_CMD} run --select intermediate",
    )

    # 4. Run dbt mart models
    t_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=f"{DBT_BASE_CMD} run --select marts",
    )

    # 5. Run all dbt tests — DAG fails if any test fails
    t_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=f"{DBT_BASE_CMD} test",
    )

    # 6. Log success summary
    t_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # Pipeline dependency chain
    (
        t_check_freshness
        >> t_dbt_staging
        >> t_dbt_intermediate
        >> t_dbt_marts
        >> t_dbt_tests
        >> t_notify
    )
