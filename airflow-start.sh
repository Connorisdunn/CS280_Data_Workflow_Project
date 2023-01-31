#!/bin/bash
export AIRFLOW_HOME=/home/connorisdunn/airflow-cs280
export GOOGLE_APPLICATION_CREDENTIALS="/home/connorisdunn/airflow-cs280/auth/bucket_auth.json"
cd /home/connorisdunn/airflow-cs280
source /home/connorisdunn/miniconda3/bin/activate
conda activate /home/connorisdunn/miniconda3/envs/airflow-env
nohup airflow scheduler >> scheduler.log &
nohup airflow webserver -p 8080 >> webserver.log &
