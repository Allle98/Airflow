import os
from socket import timeout
import pandas as pd
import datetime
import pendulum

from airflow.contrib.sensors.file_sensor import FileSensor

from airflow.models import Variable
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator



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

# =================== DAGS =======================

# pipeline setup
@dag(dag_id="ab_json_parquet", schedule_interval="@once", default_args=default_args, catchup=False, tags=['iungo'])
def taskflow():
    CURRENT = os.getcwd() + os.sep + "dags" + os.sep
    JSON = CURRENT + "input_files" + os.sep +  Variable.get("people_table", deserialize_json=True)

    OUTPUT_FILE_NAME = "person.parquet"
    OUTPUT_FILE_PATH = CURRENT + "out" + os.sep + OUTPUT_FILE_NAME  
    
    

    json_file_sensor = FileSensor(    
        task_id="json_file_sensor",
        poke_interval=1, # by default is 30 seconds
        fs_conn_id="file_connection",
        filepath=JSON,
        timeout=1,   # if we don't set a timeout my sensor will check every 30 (poke_interval) seconds during 7 days before timing out!!!
        #mode='reschedule',  # lets the other tasks work and then gets rescheduled, this to avoid deadlocks. Putting this is a best-practice
        #soft_fail=True # if my sensor is longer to execute than the time defined in timeout I want to skip the sensor
                        # if I don't want to fail. By default is set to False
    )
           

    @task(task_id="read_json")
    def read_json(filename=JSON):
        df = pd.read_json(filename)
        #print(df[['age', 'city']])

        return df.to_json()

    
    @task(task_id="json_to_paquet")
    def json_to_parquet(df: pd.DataFrame, output_file=OUTPUT_FILE_PATH):
        df = pd.read_json(df)
        df.to_parquet(output_file)

    json = read_json()
    json_file_sensor >> json
    json_to_parquet(json)
# =================== DAG =======================

tasks = taskflow()

# =================== END =======================


