import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='wikipedia_pageview_bash',
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval='@hourly',
    catchup=False,
) as dag:
    get_data = BashOperator(
        task_id='get_data',
        bash_command=(
            'curl -o /opt/airflow/tmp/wikipageviews.gz '
            'https://dumps.wikimedia.org/other/pageviews/'
            '{{ logical_date.year }}/'
            '{{ logical_date.year}}-'
            '{{ "{:02}".format(logical_date.month) }}/'
            'pageviews-{{ logical_date.year }}'
            '{{ "{:02}".format(logical_date.month) }}'
            '{{ "{:02}".format(logical_date.day) }}'
            '{{ "{:02}".format(logical_date.hour) }}0000.gz'
        )
    )

    get_data
