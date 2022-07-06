import os
from socket import timeout
import pandas as pd
import datetime
import pendulum

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

# ===============================================
default_args = {
                "owner": "ab",
                "start_date": pendulum.datetime(2022, 5, 2),
                "retries": 1,
                "retry_delay": datetime.timedelta(minutes=60),
                "email": ["test@gmail.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                }

# =================== FUNCTIONS =======================

# =================== DAGS =======================

# pipeline setup
@dag(dag_id="ab_task_group01", schedule_interval="@once", default_args=default_args, catchup=False, tags=['iungo', 'task_group'])
def main():
    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="clean_group") as clean_group:
        task1 = EmptyOperator(task_id="task1")
        task2 = EmptyOperator(task_id="task2")

    with TaskGroup(group_id="process_group") as process_group:
        task1 = EmptyOperator(task_id="task1")
        task2 = EmptyOperator(task_id="task2")

    end = EmptyOperator(task_id="end")

    start >> clean_group >> process_group >> end

# =================== DAG =======================

taskflow = main()

# =================== END =======================


