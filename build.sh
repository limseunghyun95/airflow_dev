#!/bin/bash

image=airflow:2.9.3

docker build . -f Dockerfile -t airflow:2.9.3 &&
docker compose up