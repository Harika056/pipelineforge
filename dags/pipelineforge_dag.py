"""Airflow DAG wrapping the batch pipeline.

Checked with `airflow dags test pipelineforge_batch` on Airflow 3.3.2 (Linux): both tasks succeeded.
Airflow does not run natively on Windows. Copy this file into your Airflow `dags/` folder (with the
`pipelineforge` package importable). The same logic runs without Airflow via
`python -m pipelineforge.run batch`.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

try:                                   # Airflow 3
    from airflow.sdk import dag, task
except ImportError:                    # Airflow 2.x
    from airflow.decorators import dag, task

OLTP = os.getenv("PF_OLTP", "/opt/data/oltp.db")
WAREHOUSE = os.getenv("PF_WAREHOUSE", "/opt/data/warehouse.db")


@dag(dag_id="pipelineforge_batch", schedule="@hourly", start_date=datetime(2025, 1, 1), catchup=False,
     default_args={"retries": 2, "retry_delay": timedelta(minutes=2)}, tags=["etl"])
def pipelineforge_batch():
    @task
    def run_batch_task() -> dict:
        from pipelineforge.pipeline import run_batch
        return run_batch(OLTP, WAREHOUSE, engine=os.getenv("PF_ENGINE", "pandas"))

    @task
    def check_reconciliation(stats: dict) -> None:
        if stats["fact"]["unresolved"]:
            raise ValueError(f"{stats['fact']['unresolved']} items could not be resolved into the fact table")

    check_reconciliation(run_batch_task())


pipelineforge_batch()
