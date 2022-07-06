
import os
import pendulum
import datetime
import pandas as pd
from airflow import DAG
from airflow.decorators import task

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
                "email": ["test@gmail.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                }

# =================== FUNCTIONS =======================
REL_PATH = os.path.join(os.getcwd(), "dags/")

def read_file(ti):
    path = (REL_PATH +"data/netflix_titles.csv")
    df = pd.read_csv(path)
    ti.xcom_push(key='df', value=df.to_json())

def process_title(ti):
    df = pd.read_json(ti.xcom_pull(key="df"))
    df["title"]  = df['title'].apply(lambda x:  'pippo')
    ti.xcom_push(key='title', value=df["title"].to_json())

def process_director(ti):
    df = pd.read_json(ti.xcom_pull(key="df"))
    df["director"]  = df['director'].apply(lambda x:  'pluto')
    ti.xcom_push(key='director', value= df["director"].to_json())

def process_type(ti):
    df = pd.read_json(ti.xcom_pull(key="df"))
    df["type"]  = df['type'].apply(lambda x: 'paperino')
    ti.xcom_push(key='type', value=df["type"].to_json())

def complete_task(ti):
    df = pd.DataFrame(pd.read_json(ti.xcom_pull(key="df")))
    df["type"] = pd.read_json((ti.xcom_pull(key="type")), typ='series', orient='records')
    df["title"] = pd.read_json((ti.xcom_pull(key="title")), typ='series', orient='records')
    df["director"] = pd.read_json((ti.xcom_pull(key="director")), typ='series', orient='records')

    path = (REL_PATH + "results/process.csv")
    df.to_csv(path)

    ti.xcom_push(key='df', value=df.to_json())
# =================== DAGS =======================

dag = DAG( dag_id="03_pandas", 
          schedule_interval="@once",#'@daily',
          default_args=default_args,
          catchup=False,
          tags=['iungo']
           )

read_file = PythonOperator(task_id="read_file",
                            python_callable=read_file,
                            dag=dag,
                            )

process_title = PythonOperator(task_id="process_title",
                                python_callable=process_title,
                                dag=dag,
                                )
process_director = PythonOperator(task_id="process_director",
                                  python_callable=process_director,
                                  dag=dag,
                                  )
process_type = PythonOperator(task_id="process_type",
                              python_callable=process_type,
                              dag=dag,
                              )

complete_task = PythonOperator(task_id="complete_task",
                                python_callable=complete_task,
                                dag=dag,
                                )

# =================== SCHEMA =======================

read_file >> [process_title,process_director,process_type] >> complete_task


# ==================== END =========================

