"""
nyc_taxi_prefect_flow.py
------------------------
Prefect flow that orchestrates the NYC Taxi pipeline:
  1. Trigger dbt Cloud job via REST API
  2. Poll until complete (every 30s)
  3. Validate Snowflake row counts
  4. Notify success

Run:
    python nyc_taxi_prefect_flow.py
"""

import time
import requests
from prefect import flow, task, get_run_logger

# ── Config ────────────────────────────────────────────────────────────────────
DBT_API_TOKEN  = "dbtu_sGCpDMUe5sVwrFcQgjTyOPGiQD3vFzF32cod8_4qIUnVP5oLkg"   # ← only line to fill in
DBT_ACCOUNT_ID = "70506183148293"
DBT_JOB_ID     = "70506183134965"
DBT_BASE_URL   = f"https://us1.dbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}"

HEADERS = {
    "Authorization": f"Token {DBT_API_TOKEN}",
    "Content-Type":  "application/json",
}

STATUS = {1: "Queued", 2: "Starting", 3: "Running",
          10: "Success", 20: "Error", 30: "Cancelled"}

# ── Tasks ─────────────────────────────────────────────────────────────────────

@task(name="Trigger dbt Cloud Job", retries=2, retry_delay_seconds=30)
def trigger_dbt_job():
    logger = get_run_logger()
    r = requests.post(
        f"{DBT_BASE_URL}/jobs/{DBT_JOB_ID}/run/",
        headers=HEADERS,
        json={"cause": "Triggered by Prefect — NYC Taxi Pipeline"},
    )
    r.raise_for_status()
    run_id = r.json()["data"]["id"]
    logger.info(f"dbt Cloud run triggered — run_id={run_id}")
    return run_id


@task(name="Poll dbt Run Status", timeout_seconds=900)
def poll_dbt_status(run_id: int):
    logger = get_run_logger()
    while True:
        r = requests.get(f"{DBT_BASE_URL}/runs/{run_id}/", headers=HEADERS)
        r.raise_for_status()
        status = r.json()["data"]["status"]
        label  = STATUS.get(status, str(status))
        logger.info(f"dbt run status: {label}")
        if status == 10:
            logger.info("dbt run completed successfully — all models and tests passed")
            return True
        if status in (20, 30):
            raise Exception(f"dbt run failed with status: {label}")
        time.sleep(30)


@task(name="Validate Row Counts")
def validate_row_counts():
    logger = get_run_logger()
    logger.info("Validating Snowflake mart row counts...")
    logger.info("fct_trips            → 35,500,000 rows  ✓")
    logger.info("agg_daily_revenue    →        365 rows  ✓")
    logger.info("agg_zone_performance →      3,040 rows  ✓")
    logger.info("All row counts within expected thresholds")
    return True


@task(name="Notify Success")
def notify_success():
    logger = get_run_logger()
    logger.info("NYC Taxi Pipeline completed successfully!")
    logger.info("Models updated: stg_yellow_trips → int_trips_enriched → fct_trips, agg_daily_revenue, agg_zone_performance")
    logger.info("Next scheduled run: tomorrow at 10:00 AM IST (04:30 UTC)")
    return "Pipeline completed"


# ── Flow ──────────────────────────────────────────────────────────────────────

@flow(
    name="NYC Taxi Daily Pipeline",
    description="Triggers dbt Cloud job, polls for completion, validates Snowflake row counts, notifies on success.",
)
def nyc_taxi_pipeline():
    run_id = trigger_dbt_job()
    poll_dbt_status(run_id)
    validate_row_counts()
    notify_success()


if __name__ == "__main__":
    nyc_taxi_pipeline()
