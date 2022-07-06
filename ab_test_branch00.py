import os
import pendulum
import random
import datetime
import pandas as pd
from airflow import DAG
from airflow.decorators import dag, task

# Operators
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
#from airflow.operators.empty import EmptyOperator

# ===============================================
default_args = {
                "owner": "ab",
                "start_date": pendulum.datetime(2022, 5, 13),
                "retries": 1,
                "retry_delay": datetime.timedelta(minutes=60),
                'email_on_failure': False,
                }

# =================== FUNCTIONS =======================
## path = os.path.join(os.getcwd(), "dags/....)

def task_true_function():
    print("Value: true")

def task_false_function():
    print("Value: false")

# ====================== DAG ==========================

# pipeline setup

dag = DAG( dag_id='ab_test_branch00',
          schedule_interval='@daily', 
          default_args=default_args,
          catchup=False,
          tags=['iungo'],
        )

task_true = PythonOperator(
    task_id="task_true",
    python_callable=task_true_function,
    dag=dag
)

task_false = PythonOperator(
    task_id="task_false",
    python_callable=task_false_function,
    dag=dag
)

options = ["task_true", "task_false"]

branch = BranchPythonOperator(
    task_id="branch",
    python_callable=lambda: random.choice(options),
    dag=dag
)


 

# =================== SCHEMA =======================

branch >> [task_true, task_false]

# ==================== END =========================
