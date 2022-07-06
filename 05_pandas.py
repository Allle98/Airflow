import pandas as pd
import datetime
import pendulum 
from airflow import DAG
from airflow.decorators import task

from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
# ===============================================
default_args = {
                "owner": "D&G",
                "start_date": pendulum.datetime(2022, 5, 2),
                "retries": 1,
                "retry_delay": datetime.timedelta(minutes=60),
                "email": ["test@gmail.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                }

# =================== FUNCTIONS =======================
URL = "https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv"
##22 mb, takes time to load

def read_csv():
    df = pd.read_csv(URL,nrows=100)
    print(f"Total was {df}")
    return df.to_json()

# =================== DAGS =======================

# pipeline setup
dag = DAG( dag_id="05_load_UCI", 
          schedule_interval="@once",#'@daily',
          default_args=default_args,
          catchup=False,
          tags=['iungo']
           )

run_python_report = PythonOperator(
                                    task_id='python_report', 
                                    python_callable=read_csv, 
                                    dag=dag
                                    )

start_op = DummyOperator(task_id='start_task', dag=dag)
end_op = DummyOperator(task_id='end_task', dag=dag)

# =================== SCHEMA =======================

start_op >> run_python_report >> end_op

# ==================== END =========================
