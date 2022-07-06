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
#from airflow.operators.python import task, get_current_context

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

def read_file(**context):
    # context = get_current_context()       # se passo il context come parametro evito questa riga
    # ti = context["ti"]                    # e questa
    path = os.path.join(os.getcwd(), "dags/data/netflix_titles.csv")
    df = pd.read_csv(path)
    context['ti'].xcom_push(key='df', value=df.to_json())

def process_title(**context):
    df = pd.read_json(context.get("ti").xcom_pull(key="df"))
    df["title"]  = df['title'].apply(lambda x:  'pippo')
    context['ti'].xcom_push(key='title', value=df["title"].to_json())
    #return df.to_dict()

def process_director(**context):
    df = pd.read_json(context.get("ti").xcom_pull(key="df"))
    df["director"]  = df['director'].apply(lambda x:  'pluto')
    context['ti'].xcom_push(key='director', value= df["director"].to_json())
    #return df.to_dict()

def process_type(**context):
    df = pd.read_json(context.get("ti").xcom_pull(key="df"))
    df["type"]  = df['type'].apply(lambda x: 'paperino')
    context['ti'].xcom_push(key='type', value=df["type"].to_json())
    #return df.to_dict()

def complete_task(**context):
    df = pd.DataFrame(pd.read_json(context.get("ti").xcom_pull(key="df")))
    df["type"] = pd.read_json((context.get("ti").xcom_pull(key="type")), typ='series', orient='records')
    df["title"] = pd.read_json((context.get("ti").xcom_pull(key="title")), typ='series', orient='records')
    df["director"] = pd.read_json((context.get("ti").xcom_pull(key="director")), typ='series', orient='records')

    path = os.path.join(os.getcwd(), "dags/results/process.csv")
    df.to_csv(path)
    # context['ti'].xcom_push(key='df', value=df.to_json())   # compare nell'xcom
    # return df.to_dict()                                     # compare nell'xcom e nel return value
# =================== DAGS =======================

dag = DAG( dag_id="02_pandas", 
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

