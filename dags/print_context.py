from urllib import request

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator


# def _get_data(**context):
#     year, month, day, hour, *_ = context['logical_date'].timetuple()
#     url = (
#         'https://dumps.wikimedia.org/other/pageviews/'
#         f'{year}/{year}-{month:0>2}/'
#         f'pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz'
#     )
#     output_path = '/opt/airflow/tmp/wikipageviews.gz'
#     request.urlretrieve(url, output_path)


# def _get_data(logical_date, **context):
#     year, month, day, hour, *_ = logical_date.timetuple()
#     url = (
#         'https://dumps.wikimedia.org/other/pageviews/'
#         f'{year}/{year}-{month:0>2}/'
#         f'pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz'
#     )
#     output_path = '/opt/airflow/tmp/wikipageviews.gz'
#     request.urlretrieve(url, output_path)


def _get_data(year, month, day, hour, output_path, **_):
    url = (
        'https://dumps.wikimedia.org/other/pageviews/'
        f'{year}/{year}-{month:0>2}/'
        f'pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz'
    )
    print(url)
    # request.urlretrieve(url, output_path)


with DAG(
    dag_id='print_context',
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval='@hourly',
    catchup=False,
) as dag:
    # get_data = PythonOperator(
    #     task_id='get_data',
    #     python_callable=_get_data
    # )

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=_get_data,
        op_kwargs={
            'year': '{{ logical_date.year }}',
            'month': '{{ logical_date.month }}',
            'day': '{{ logical_date.day }}',
            'hour': '{{ logical_date.hour }}',
            'output_path': '/opt/airflow/tmp/wikipageviews.gz',
        }
    )

    get_data
