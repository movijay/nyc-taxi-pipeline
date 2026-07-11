"""
nyc_taxi_daily_pipeline
-----------------------
Airflow DAG that orchestrates the NYC Taxi dbt Cloud pipeline daily at 02:00 UTC.

Architecture:
  - Airflow (Astronomer Astro) handles scheduling and orchestration
  - dbt Cloud handles all transformations and tests (triggered via REST API)
  - Snowflake is the data warehouse (managed by dbt Cloud)

No extra providers needed — uses only the built-in `requests` library.

Setup required in Airflow UI (Admin → Variables):
  - dbt_cloud_api_token  : your dbt Cloud API token
"""

import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DBT_ACCOUNT_ID = "70506183148293"
DBT_JOB_ID     = "70506183134965"
DBT_BASE_URL   = "https://ej165.us1.dbt.com"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ---------------------------------------------------------------------------
# Task: trigger dbt Cloud job and wait for completion
# ---------------------------------------------------------------------------
def trigger_and_wait_dbt_job(**kwargs):
    api_token = Variable.get("dbt_cloud_api_token")
    headers = {
        "Authorization": f"Token {api_token}",
        "Content-Type": "application/json",
    }

    # 1. Trigger the job
    trigger_url = f"{DBT_BASE_URL}/api/v2/accounts/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/"
    resp = requests.post(
        trigger_url,
        headers=headers,
        json={"cause": "Triggered by Airflow — NYC Taxi Pipeline"},
    )
    resp.raise_for_status()
    run_id = resp.json()["data"]["id"]
    print(f"[INFO] dbt Cloud job triggered. Run ID: {run_id}")
    print(f"[INFO] Monitor at: {DBT_BASE_URL}/deploy/{DBT_ACCOUNT_ID}/runs/{run_id}")

    # 2. Poll until done
    # Status codes: 1=Queued, 2=Starting, 3=Running, 10=Success, 20=Error, 30=Cancelled
    status_url = f"{DBT_BASE_URL}/api/v2/accounts/{DBT_ACCOUNT_ID}/runs/{run_id}/"
    while True:
        time.sleep(30)
        status_resp = requests.get(status_url, headers=headers)
        status_resp.raise_for_status()
        run_data = status_resp.json()["data"]
        status = run_data["status"]
        status_label = run_data.get("status_humanized", str(status))
        print(f"[INFO] Run {run_id} status: {status_label}")

        if status == 10:
            print("[SUCCESS] dbt Cloud job completed successfully!")
            print(f"[INFO]    Models run:  {run_data.get('run_steps', [])}")
            break
        elif status in (20, 30):
            raise Exception(
                f"dbt Cloud job failed. Status: {status_label}. "
                f"Check run at: {DBT_BASE_URL}/deploy/{DBT_ACCOUNT_ID}/runs/{run_id}"
            )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_daily_pipeline",
    description="NYC Taxi TLC: trigger dbt Cloud run+test via REST API, no extra providers needed",
    schedule="0 2 * * *",             # 02:00 UTC daily (Airflow 3.x syntax)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["nyc-taxi", "dbt", "snowflake", "data-engineering"],
) as dag:

    run_dbt_pipeline = PythonOperator(
        task_id="run_dbt_pipeline",
        python_callable=trigger_and_wait_dbt_job,
    )
