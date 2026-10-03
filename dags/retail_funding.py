"""Daily scheduled processing; synthetic generation is a separate manual DAG."""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "retail_funding_daily",
    start_date=datetime(2026, 10, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    dagrun_timeout=timedelta(hours=4),
    tags=["retail", "funding", "reconciliation"],
) as dag:
    process = BashOperator(
        task_id="funding_and_reconciliation",
        bash_command='python -m retail_flow.cli run --root /opt/airflow/data --date "$BATCH_DATE" --format parquet',
        env={"BATCH_DATE": "{{ ds }}"},
        append_env=True,
        execution_timeout=timedelta(hours=4),
    )
    report = BashOperator(
        task_id="publish_audit_report",
        bash_command='python -m retail_flow.cli report --root /opt/airflow/data --date "$BATCH_DATE"',
        env={"BATCH_DATE": "{{ ds }}"},
        append_env=True,
    )
    process >> report

with DAG(
    "retail_synthetic_demo",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["retail", "synthetic"],
) as demo:
    BashOperator(
        task_id="generate_and_process",
        bash_command='python -m retail_flow.cli demo --root /opt/airflow/data --date "$BATCH_DATE" --rows 1000',
        env={"BATCH_DATE": "{{ ds }}"},
        append_env=True,
    )
