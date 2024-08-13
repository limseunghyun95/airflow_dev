from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from event_generator import get_events


dag = DAG(
    dag_id="02_incremental",
    start_date=datetime(2019, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

fetch_events = PythonOperator(
    task_id="fetch_events",
    python_callable=get_events,
    op_kwargs={
        'start_date': "{{ data_interval_start.strftime('%Y-%m-%d') }}",
        'end_date': "{{ data_interval_end.strftime('%Y-%m-%d') }}",
        "output_file": "/opt/airflow/tmp/events_{{ data_interval_start.strftime('%Y-%m-%d') }}.json",
    },
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
    op_kwargs={"input_path": "/opt/airflow/tmp/events_{{ data_interval_start.strftime('%Y-%m-%d') }}.json",
               "output_path": "/opt/airflow/tmp/stats_{{ data_interval_start.strftime('%Y-%m-%d') }}.csv"},
    dag=dag,
)


# def _calculate_stats(**context):
#     """Calculates event statistics."""
#     input_path = context["templates_dict"]["input_path"]
#     output_path = context["templates_dict"]["output_path"]

#     events = pd.read_json(input_path)
#     stats = events.groupby(["date", "user"]).size().reset_index()

#     Path(output_path).parent.mkdir(exist_ok=True)
#     stats.to_csv(output_path, index=False)
    

# calculate_stats = PythonOperator(
#     task_id="calculate_stats",
#     python_callable=_calculate_stats,
#     templates_dict={
#         "input_path": "/opt/airflow/tmp/events_{{ data_interval_start.strftime('%Y-%m-%d') }}.json",
#         "output_path": "/opt/airflow/tmp/stats_{{ data_interval_start.strftime('%Y-%m-%d') }}.csv",
#     },
#     # Required in Airflow 1.10 to access templates_dict, deprecated in Airflow 2+.
#     # provide_context=True,
#     dag=dag,
# )


fetch_events >> calculate_stats
