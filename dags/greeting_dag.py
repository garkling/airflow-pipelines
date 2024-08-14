import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def greeting(name):
    logging.info(f"Hello, {name}!")


with DAG(dag_id="greeting_dag", schedule_interval="@daily", start_date=datetime(2024, 8, 9)) as dag:
    greeting_task = PythonOperator(
        task_id="greeting_task",
        python_callable=greeting,
        op_args=("Vlad",)
    )
