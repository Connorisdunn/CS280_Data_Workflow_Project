#!/bin/bash
export AIRFLOW_HOME=/home/<Your User Here>/airflow-cs280
cd /home/<Your User Here>/airflow-cs280
source /home/<Your User Here>/miniconda3/bin/activate
conda activate /home/<Your User Here>/miniconda3/envs/airflow-env
nohup airflow scheduler >> scheduler.log &
nohup airflow webserver -p 8080 >> webserver.log &
