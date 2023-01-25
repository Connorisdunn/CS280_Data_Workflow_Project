from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

def first_task_function():
    log.info("Hello this will be my attempt at making a DAG")
    name = "Enter your favorite basketball player's name here"
    log.info(f"Their name is {name}")
    return

def second_task_function():
    log.info("This is your second task")
    number = "Enter their number here"
    log.info(f"Their number is {number}")
    return

def third_task_function():
    log.info("This is your third task")
    team = "Enter what team they play for here"
    log.info(f"They play for {team}")
    return

with DAG(
    dag_id="Favorite basketball player",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start_task")
    first_task = PythonOperator(task_id="first_task", python_callable=first_task_function)
    second_task = PythonOperator(task_id="second_task", python_callable=second_task_function)
    third_task = PythonOperator(task_id="third_task", python_callable=third_task_function)
    end_task = DummyOperator(task_id="end_task")

start_task >> first_task >> [second_task, third_task] >> end_task
