from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task

from random import uniform
import datetime
import pendulum

# ======================================================
default_args = {
                "owner": "D&G",
                "start_date": pendulum.datetime(2022, 5, 2),
                "retries": 1,
                "retry_delay": datetime.timedelta(minutes=60),
                'email_on_failure': False,
                }

# =================== FUNCTIONS =======================
## path = os.path.join(os.getcwd(), "dags/....)

def training_model(ti):  # task instance object
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')
    ti.xcom_push(key='model_accuracy', value=accuracy)

def choose_best_model(ti):
    print('choose best model')
    accuracies = ti.xcom_pull(key='model_accuracy', task_ids=['training_model_A', 'training_model_B', 'training_model_C'])
    print(accuracies)

# =================== DAGS =======================

# pipeline setup

dag = DAG( dag_id='01_testdag',
          schedule_interval='@daily', 
          default_args=default_args,
          catchup=False,
          tags=['iungo'],
        )

downloading_data = BashOperator(
                                task_id='downloading_data',
                                bash_command='sleep 3',
                                do_xcom_push=False,
                                dag = dag
                                )

training_model_task = [PythonOperator(
                                    task_id=f'training_model_{task}',
                                    python_callable = training_model,
                                    dag = dag
                                    ) for task in ['A', 'B', 'C']]

choose_model = PythonOperator(
                              task_id='choose_model',
                              python_callable = choose_best_model,
                              dag = dag
                              )
                              
# =================== SCHEMA =======================

downloading_data >> training_model_task >> choose_model

# ==================== END =========================