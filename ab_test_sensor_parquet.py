import os
from socket import timeout
import pandas as pd
import datetime
import pendulum
import time
from airflow.decorators import dag, task

from airflow.operators.empty import EmptyOperator
from airflow.contrib.sensors.file_sensor import FileSensor

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

# =================== GLOBAL VARIABLES =======================

CURRENT = os.getcwd() + os.sep + "dags" + os.sep

INPUT_FILE_NAME = "online_retail.csv"
INPUT_FILE_PATH = CURRENT + "biella" + os.sep + INPUT_FILE_NAME

OUTPUT_FILE_NAME = "out.parquet"
OUTPUT_FILE_PATH = CURRENT + "out" + os.sep + OUTPUT_FILE_NAME

# =================== TASKS =======================

# pipeline setup
@dag(dag_id="ab_test_sensor_parquet", schedule_interval="@once", default_args=default_args, catchup=False, tags=['iungo'])
def main():

    start = EmptyOperator(task_id="start")

    sensor_check_input = FileSensor(    
        task_id="sensor_check_file",
        poke_interval=30, # by default is 30 seconds
        fs_conn_id="airflow_db",
        filepath=INPUT_FILE_PATH,
        timeout=120
    )

    @task(task_id='read_csv')
    def read_csv():
        df = pd.read_csv(INPUT_FILE_PATH, nrows=100)
        print(f"My dataframe is {df}")

        # to see how much time takes
        start_time = time.time()
        result = df.query('Quantity < 8', inplace=False)
        print("Execution time query on csv: %s seconds " % (time.time() - start_time))
        print(result)

        return df.to_json()

    @task(task_id='store_parquet')
    def store_parquet(df: pd.DataFrame, output_file: str=OUTPUT_FILE_PATH):
        df = pd.read_json(df) ### provare a togliere
        df.to_parquet(output_file)


    json = read_csv()
    start >> sensor_check_input >> json >> store_parquet(json)
    
    # sensor_check_input >> json >> store_parquet(json)



    #sensor_check_input(start())
# =================== DAG =======================

graph = main()

# =================== END =======================