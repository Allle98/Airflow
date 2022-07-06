from datetime import datetime
import os
import pandas as pd
import requests

from airflow.decorators import dag
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.bash_operator import BashOperator


API = "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"
DATA_FILE = 'online_retail.csv'
FILE_PATH = os.getcwd() + os.sep + "dags" + os.sep + "GCP" + os.sep + DATA_FILE
BUCKET_NAME = "test_airflow_infomanager"

@dag(dag_id='ab_api_to_bucket', schedule_interval='@once', start_date=datetime(2022, 1, 1), tags=['iungo', 'gcs', 'bucket'], catchup=False)
def taskflow():
    
    def extract_data(url: str = API, file_path=FILE_PATH):
        df = pd.read_excel(url, nrows=1000,)
        df.to_csv(file_path, index=False)

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    check_cvs_exists = FileSensor(    
        task_id="check_cvs_exists",
        poke_interval=30,
        fs_conn_id="airflow_db",
        filepath=FILE_PATH,
        timeout=120
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=FILE_PATH,
        dst=DATA_FILE,
        bucket=BUCKET_NAME,
    )

    extract_data >> check_cvs_exists >> upload_file
    
graph = taskflow()
