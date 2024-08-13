import json
import os

import airflow
import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

raw_data_path = '/tmp/launches.json'
image_dir = '/tmp/images'

# DAG 생성
dag = DAG(
    dag_id='download_rocket_launches',  # Webserver 에서 표시되는 dag 이름
    start_date=airflow.utils.dates.days_ago(14),  # 최초 dag 실행 시간
    schedule_interval=None  # dag 스케쥴
)

download_launches = BashOperator(
    task_id='download_launches',
    bash_command=f'curl -o {raw_data_path} -L "https://ll.thespacedevs.com/2.2.0/event/upcoming"', dag=dag
)


def _get_pictures():
    # 이미지 디렉토리 생성
    if not os.path.exists(image_dir):
        os.mkdir(image_dir)

    # 이미지 다운로드
    with open(raw_data_path) as f:
        launches = json.load(f)
        image_urls = [launch['feature_image'] for launch in launches['results']]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_file_name = image_url.split('/')[-1]
                target_file = f'{image_dir}/{image_file_name}'
                with open(target_file, 'wb') as f:
                    f.write(response.content)
                print(f'Downloaded {image_url} to {target_file}')
            except requests.exceptions.MissingSchema:
                print(f'{image_url} appears to be an invalid URL')
            except requests.exceptions.ConnectionError:
                print(f'Could not connect to {image_url}')


get_pictures = PythonOperator(
    task_id='get_pictures',
    python_callable=_get_pictures,
    dag=dag,
)


notify = BashOperator(
    task_id='nofity',
    bash_command=f'echo "There are now $(ls {image_dir} | wc - l) images."'
)


download_launches >> get_pictures >> notify