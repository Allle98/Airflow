import os
from platform import python_branch
import pendulum
import random
import datetime
import pandas as pd
from airflow import DAG

# Operators
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
#from airflow.operators.empty import EmptyOperator

# Goals: execute BI Report on Mondays, notify if data is not present

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

def is_monday():

    flag = 0

    if flag:
        return 'create_report'

    return 'none'

def create_report():
    # ...
    print("Report created")

# ====================== DAG ==========================

# pipeline setup


dag = DAG( 
    dag_id='ab_test_branch01',
    schedule_interval='@daily', 
    default_args=default_args,
    catchup=False,
    tags=['iungo'],
    )


is_monday_task = BranchPythonOperator(
    task_id='is_monday',
    python_callable=is_monday,
    #provide_context=True,
    dag=dag
)

create_report_task = PythonOperator(
    task_id='create_report',
    python_callable=create_report,
    # provide_context=True,
    dag=dag
)

none = EmptyOperator(
    task_id='none',
    dag=dag
)

 
# =================== SCHEMA =======================

is_monday_task >> [create_report_task, none]

# ==================== END =========================
