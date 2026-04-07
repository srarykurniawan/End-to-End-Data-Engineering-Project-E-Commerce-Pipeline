"""
DAG: ecommerce_data_quality_monitor (v2)
Schedule: Every 6 hours — comprehensive DQ monitoring
"""

import os, sys, logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner":"data_engineering","depends_on_past":False,
    "start_date":datetime(2024,1,1),"retries":1,
    "retry_delay":timedelta(minutes=3),
    "execution_timeout":timedelta(minutes=20),
}


def task_run_full_dq(**ctx):
    """Run all active DQ rules from quality.dq_rules catalog"""
    os.environ["POSTGRES_HOST"] = "postgres"
    os.environ["POSTGRES_PORT"] = "5432"
    sys.path.insert(0, "/opt/airflow/scripts")

    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "dq_runner", "/opt/airflow/scripts/data_quality_runner.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    result = mod.run_all_checks(dag_id="ecommerce_data_quality_monitor")
    ctx["ti"].xcom_push(key="dq_result", value=result)

    # Warn but don't fail on non-critical
    if result["critical_failures"] > 0:
        raise ValueError(
            f"CRITICAL DQ failures: {result['critical_failures']}. "
            f"Health: {result['health_score']:.1f}%"
        )
    logger.info(f"DQ Monitor complete | Health: {result['health_score']:.1f}%")


def task_detect_anomalies(**ctx):
    """Compare current run vs previous run to detect anomalies"""
    import psycopg2
    conn = psycopg2.connect(
        host="postgres", port=5432, dbname="ecommerce_db",
        user="postgres", password="postgres"
    )
    cur = conn.cursor()

    # Find rules that failed this run but passed last time (new failures)
    cur.execute("""
        WITH current_run AS (
            SELECT rule_name, status, failure_pct
            FROM quality.dq_results
            WHERE run_id = (SELECT run_id FROM quality.dq_run_summary ORDER BY started_at DESC LIMIT 1)
        ),
        prev_run AS (
            SELECT rule_name, status, failure_pct
            FROM quality.dq_results
            WHERE run_id = (SELECT run_id FROM quality.dq_run_summary ORDER BY started_at DESC LIMIT 1 OFFSET 1)
        )
        SELECT c.rule_name, c.status, c.failure_pct, p.status AS prev_status
        FROM current_run c
        LEFT JOIN prev_run p ON c.rule_name = p.rule_name
        WHERE c.status = 'FAIL' AND (p.status IS NULL OR p.status = 'PASS')
    """)
    new_failures = cur.fetchall()

    for rule_name, status, pct, prev_status in new_failures:
        cur.execute("""
            INSERT INTO quality.dq_anomalies
                (rule_name, anomaly_type, description)
            VALUES (%s, 'new_failure',
                    %s)
        """, (rule_name,
              f"Rule '{rule_name}' newly failed (prev: {prev_status}, failure: {pct:.2f}%)"))
        logger.warning(f"⚠️  New anomaly: {rule_name} — {pct:.2f}% failures")

    conn.commit(); cur.close(); conn.close()
    logger.info(f"Anomaly detection: {len(new_failures)} new failures detected")


with DAG(
    dag_id="ecommerce_data_quality_monitor",
    default_args=default_args,
    schedule_interval="0 */6 * * *",  # Every 6 hours
    catchup=False, max_active_runs=1,
    tags=["ecommerce","dq","monitoring"],
    description="Comprehensive DQ monitoring with anomaly detection",
) as dag:

    start  = DummyOperator(task_id="start")
    end    = DummyOperator(task_id="end")
    run_dq = PythonOperator(task_id="run_full_dq_checks",   python_callable=task_run_full_dq)
    detect = PythonOperator(task_id="detect_anomalies",     python_callable=task_detect_anomalies)

    start >> run_dq >> detect >> end
