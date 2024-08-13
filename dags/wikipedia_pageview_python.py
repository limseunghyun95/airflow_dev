from urllib import request

import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


def _get_data(output_path, logical_date, **context):
    year, month, day, hour, *_ = (logical_date.subtract(hours=5)).timetuple()
    url = (
        'https://dumps.wikimedia.org/other/pageviews/'
        f'{year}/{year}-{month:0>2}/'
        f'pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz'
    )
    request.urlretrieve(url, output_path)


def _fetch_pageviews(pagenames, input_path, sql_path, logical_date, **context):
    result = dict.fromkeys(pagenames, 0)
    with open(input_path, 'r') as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(' ')
            if domain_code == 'en' and page_title in pagenames:
                result[page_title] = view_counts

    execution_date = logical_date.subtract(hours=5)
    with open(sql_path, 'w') as f:
        for pagename, pageviewcount in result.items():
            f.write(
                'INSERT INTO airflow.pageview_counts'
                f" VALUES ('{pagename}', {pageviewcount}, '{execution_date}');\n"
            )


with DAG(
    dag_id='wikipedia_pageview_python',
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval='@hourly',
    catchup=False,
    template_searchpath='/opt/airflow/tmp'
) as dag:
    get_data = PythonOperator(
        task_id='get_data',
        python_callable=_get_data,
        op_kwargs={'output_path': '/opt/airflow/tmp/wikipageviews.gz'}
    )

    extract_gz = BashOperator(
        task_id='extract_gz',
        bash_command='gunzip --force /opt/airflow/tmp/wikipageviews.gz'
    )

    fetch_pageview = PythonOperator(
        task_id='fetch_pageview',
        python_callable=_fetch_pageviews,
        op_kwargs={
            'pagenames': {
                'Google',
                'Amazon',
                'Apple',
                'Microsoft',
                'Facebook',
            },
            'input_path': '/opt/airflow/tmp/wikipageviews',
            'sql_path': '/opt/airflow/tmp/wikipageviews.sql'
        }
    )

    write_to_postgres = PostgresOperator(
        task_id='write_to_postgres',
        postgres_conn_id='postgres_conn',
        sql='wikipageviews.sql',
    )

    get_data >> extract_gz >> fetch_pageview >> write_to_postgres
