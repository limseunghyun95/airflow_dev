from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from event_generator import get_events

dag = DAG(
    dag_id="01_unscheduled",
    start_date=datetime(2019, 1, 1),
    schedule_interval=None  # 현재는 스케쥴링 되지 않음
)

fetch_events = PythonOperator(
    task_id="fetch_events",
    python_callable=get_events,
    dag=dag,
)


def _calculate_stats(input_path, output_path):
    """Calculates event statistics."""

    Path(output_path).parent.mkdir(exist_ok=True)

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()

    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={"input_path": "/opt/airflow/tmp/events.json",
               "output_path": "/opt/airflow/tmp/stats.csv"},
    dag=dag,
)

fetch_events >> calculate_stats
