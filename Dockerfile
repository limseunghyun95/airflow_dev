FROM apache/airflow:2.9.3-python3.9
LABEL maintainer seunghyun.lim <limseunghyun95@gmail.com>

COPY requirements.txt /
RUN pip install -r /requirements.txt

