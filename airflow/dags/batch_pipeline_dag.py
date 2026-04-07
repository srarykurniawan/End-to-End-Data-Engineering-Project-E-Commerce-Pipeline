"""
DAG: ecommerce_batch_pipeline (v3 — Robust Batch)
Schedule: Daily 01:00 WIB
Flow:
ingest → data_quality_raw → dbt → SCD2 → dbt_test → notify
"""

import os
import sys
import uuid
import logging
from datetime import datetime, timedelta
from contextlib import closing

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# DAG CONFIG
# ------------------------------------------------------------------

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
}


POSTGRES_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", 5432)),
    "dbname": os.environ.get("POSTGRES_DB", "ecommerce_db"),
    "user": os.environ.get("POSTGRES_USER", "postgres"),
    "password": os.environ.get("POSTGRES_PASSWORD", "postgres"),
}


# ------------------------------------------------------------------
# DATABASE CONNECTION
# ------------------------------------------------------------------

def get_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


# ------------------------------------------------------------------
# TASK 1 — INGEST CSV
# ------------------------------------------------------------------

def task_ingest(**context):
    run_id = str(uuid.uuid4())
    context["ti"].xcom_push(key="run_id", value=run_id)

    try:
        sys.path.insert(0, "/opt/airflow/ingestion")
        from batch_ingest import run_batch_ingest

        result = run_batch_ingest(
            csv_path="/opt/airflow/data/Ecommerce_Purchases.csv",
            batch_size=1000,
            truncate=True
        )

        logger.info("Ingest completed")
        logger.info(result)

    except Exception as e:
        logger.error("Ingest failed")
        raise e


# ------------------------------------------------------------------
# TASK 2 — DATA QUALITY CHECK
# ------------------------------------------------------------------

def task_run_dq(**context):

    run_id = context["ti"].xcom_pull(
        key="run_id",
        task_ids="ingest_csv_to_postgres"
    )

    try:
        sys.path.insert(0, "/opt/airflow/scripts")

        import importlib.util

        spec = importlib.util.spec_from_file_location(
            "dq_runner",
            "/opt/airflow/scripts/data_quality_runner.py"
        )

        dq_mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(dq_mod)

        result = dq_mod.run_all_checks(
            dag_id="ecommerce_batch_pipeline"
        )

        context["ti"].xcom_push(key="dq_result", value=result)

        if result["critical_failures"] > 0:
            raise ValueError(
                f"DQ FAILED | critical={result['critical_failures']} "
                f"health={result['health_score']:.2f}%"
            )

        logger.info(f"DQ passed | health={result['health_score']}")

    except Exception as e:
        logger.error("DQ check failed")
        raise e


# ------------------------------------------------------------------
# TASK 3 — EXPIRE SCD2
# ------------------------------------------------------------------

def task_expire_scd2():

    query = """
        WITH ranked AS (
            SELECT
                customer_key,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id
                    ORDER BY version DESC
                ) AS rn
            FROM dw.dim_customer_scd2
            WHERE is_current = TRUE
        )
        UPDATE dw.dim_customer_scd2 d
        SET
            expiry_date = NOW(),
            is_current = FALSE,
            dw_updated_at = NOW()
        FROM ranked r
        WHERE d.customer_key = r.customer_key
        AND r.rn > 1
    """

    try:
        with closing(get_connection()) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                expired = cur.rowcount
                conn.commit()

        logger.info(f"SCD2 expired rows: {expired}")

    except Exception as e:
        logger.error("SCD2 expiration failed")
        raise e


# ------------------------------------------------------------------
# TASK 4 — NOTIFICATION
# ------------------------------------------------------------------

def task_notify(**context):

    run_id = context["ti"].xcom_pull(
        key="run_id",
        task_ids="ingest_csv_to_postgres"
    )

    dq_result = context["ti"].xcom_pull(
        key="dq_result",
        task_ids="run_data_quality"
    )

    query = """
        SELECT
            (SELECT COUNT(*) FROM raw.ecommerce_purchases),
            (SELECT COUNT(*) FROM staging.stg_purchases),
            (SELECT COUNT(*) FROM marts.mart_customer_360),
            (SELECT COUNT(*) FROM marts.mart_fraud_signals),
            (SELECT COUNT(*) FROM dw.dim_customer_scd2 WHERE is_current=TRUE),
            (SELECT COUNT(*) FROM dw.fact_transactions)
    """

    try:
        with closing(get_connection()) as conn:
            with conn.cursor() as cur:

                cur.execute(query)
                result = cur.fetchone()

                cur.execute(
                    """
                    INSERT INTO monitoring.pipeline_runs
                    (run_id, dag_id, task_id, status, records_written, created_at)
                    VALUES (%s,'ecommerce_batch_pipeline','notify','success',%s,NOW())
                    """,
                    (run_id, result[5])
                )

                conn.commit()

        logger.info(
            f"""
Pipeline Completed
raw={result[0]}
staging={result[1]}
mart_customer={result[2]}
fraud={result[3]}
dim_customer={result[4]}
fact_transactions={result[5]}
DQ health={dq_result['health_score'] if dq_result else 'N/A'}
"""
        )

    except Exception as e:
        logger.error("Notification task failed")
        raise e


# ------------------------------------------------------------------
# DBT COMMAND TEMPLATE
# ------------------------------------------------------------------

DBT_CMD = """
set -e
cd /opt/airflow/dbt

export POSTGRES_HOST=postgres
export POSTGRES_PORT=5432

dbt {command} \
--profiles-dir /opt/airflow/dbt \
--project-dir /opt/airflow/dbt \
--target prod
"""


# ------------------------------------------------------------------
# DAG DEFINITION
# ------------------------------------------------------------------

with DAG(
    dag_id="ecommerce_batch_pipeline",
    default_args=default_args,
    schedule_interval="0 18 * * *",  # 01:00 WIB
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "batch", "production"],
) as dag:

    start = EmptyOperator(task_id="start")

    end = EmptyOperator(task_id="end")

    ingest = PythonOperator(
        task_id="ingest_csv_to_postgres",
        python_callable=task_ingest
    )

    run_dq = PythonOperator(
        task_id="run_data_quality",
        python_callable=task_run_dq
    )

    dbt_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=DBT_CMD.format(
            command="run --select staging intermediate"
        )
    )

    dbt_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=DBT_CMD.format(
            command="run --select marts"
        )
    )

    dbt_dw = BashOperator(
        task_id="dbt_run_dw",
        bash_command=DBT_CMD.format(
            command="run --select dw"
        )
    )

    expire_scd2 = PythonOperator(
        task_id="expire_scd2",
        python_callable=task_expire_scd2
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_CMD.format(
            command="test"
        )
    )

    notify = PythonOperator(
        task_id="notify_complete",
        python_callable=task_notify
    )

    (
        start
        >> ingest
        >> run_dq
        >> dbt_staging
        >> dbt_marts
        >> dbt_dw
        >> expire_scd2
        >> dbt_test
        >> notify
        >> end
    )