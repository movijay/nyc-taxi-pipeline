"""
nyc_taxi_daily_pipeline
-----------------------
Airflow DAG that orchestrates the NYC Taxi dbt Cloud pipeline daily at 02:00 UTC.

Architecture:
  - Airflow (Astronomer Astro) handles scheduling and orchestration
  - dbt Cloud handles all transformations and tests (triggered via API)
  - Snowflake is the data warehouse

Connections required in Airflow UI:
  - dbt_cloud_default  : dbt Cloud connection (API token)
  - snowflake_default  : Snowflake connection (for notify task)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DBT_CLOUD_CONN_ID = "dbt_cloud_default"
DBT_JOB_ID        = 70506183134965        # NYC Taxi -- Run + Test job in dbt Cloud
SNOWFLAKE_CONN_ID = "snowflake_default"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------
def notify_success(**kwargs):
    """
    Queries Snowflake mart layer and logs a pipeline success summary.
    Uses SnowflakeHook so no local DB required.
    """
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    result = hook.get_first("""
        SELECT
            SUM(total_trips)  AS total_trips,
            SUM(total_fare)   AS total_fare
        FROM NYC_TAXI.RAW_MARTS.agg_daily_revenue
    """)

    if result and result[0]:
        trips, revenue = result
        print(f"[SUCCESS] Pipeline complete -- {int(trips):,} total trips | ${float(revenue):,.2f} total fare in mart")
    else:
        print("[SUCCESS] Pipeline complete -- mart query returned no rows (check mart build).")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_daily_pipeline",
    description="NYC Taxi TLC: trigger dbt Cloud run+test, then notify via Snowflake",
    schedule_interval="0 2 * * *",   # 02:00 UTC daily
    start_date=datetime(2024, 1, 1),
    catchup=False,                   # no backfill -- data is already loaded
    default_args=DEFAULT_ARGS,
    tags=["nyc-taxi", "dbt", "snowflake", "data-engineering"],
) as dag:

    # 1. Trigger dbt Cloud job (runs dbt run + dbt test)
    t_dbt_run = DbtCloudRunJobOperator(
        task_id="run_dbt_pipeline",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=DBT_JOB_ID,
        check_interval=30,   # poll every 30s
        timeout=3600,        # fail if not done in 1 hour
    )

    # 2. Query Snowflake and log success summary
    t_notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    # Pipeline chain
    t_dbt_run >> t_notify
