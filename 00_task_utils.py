import os
import pendulum
import datetime
import pandas as pd
from airflow import DAG
from airflow.decorators import dag, task

# Operators
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
#from airflow.operators.empty import EmptyOperator

# ===============================================
default_args = {
                "owner": "D&G",
                "start_date": pendulum.datetime(2022, 5, 2),
                "retries": 1,
                "retry_delay": datetime.timedelta(minutes=60),
                'email_on_failure': False,
                }

# =================== FUNCTIONS =======================
## path = os.path.join(os.getcwd(), "dags/....)

from utils._math import add_one,double

@dag(
    dag_id="00_task_utils", 
    schedule_interval="@once",#'@daily',
    default_args=default_args,
    catchup=False,
    tags=['iungo']
    )

def main():
    ''' test task '''
    #added_values = add_one(11.3)
    #double(added_values)
    double(add_one(11.3))

# =================== DAG =======================

pippo = main()

# ==================== END =========================


#add_one >> double