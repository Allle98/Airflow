import os
from socket import timeout
import pandas as pd
import datetime
import pendulum

from google.oauth2.service_account import Credentials
from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


default_args = {
    "owner": "ab",
    "start_date": pendulum.datetime(2022, 5, 2),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=60),
    "email": ["test@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}


WORKING_DIRECTORY = os.getcwd() + os.sep + "dags" + os.sep + "sample1" + os.sep
FILE_PATH =  WORKING_DIRECTORY + "sample1.xlsx"
SHEET_NAME = "DB_tot"
OUTPUT_JSON = "sample1.json"

PROJECT_ID = "iungo-infomanager"
DATASET = "sample1"
TABLE = "DB_tot"

SECRET_FILE = os.getcwd() + os.sep + "dags" + os.sep + "credentials" + os.sep + "iungo-infomanager.json"

@dag(dag_id="sample1", schedule_interval="@once", default_args=default_args, catchup=False, tags=['iungo', 'sample'])
def taskflow():

    @task(task_id='excel_to_json', retries=1)
    def excel_to_json(file_path: str = FILE_PATH, sheet_name: str = SHEET_NAME):
        df = pd.read_excel(file_path, sheet_name)
        print(f"Total dataframe is {df}")
        df.to_json((WORKING_DIRECTORY + OUTPUT_JSON))
    

    @task(task_id="json_to_bq")
    def json_to_bq(file_path: str = (WORKING_DIRECTORY + OUTPUT_JSON)):
        credential = Credentials.from_service_account_file(SECRET_FILE)
        
        df = pd.read_json(file_path)
        df.to_gbq(f"{DATASET}.{TABLE}", if_exists='append', progress_bar=True, credentials=credential)

   
    delete_duplicate = BigQueryInsertJobOperator(
        task_id="delete_duplicate",
        configuration={
            "query": {
                "query": f"CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.{TABLE}`"
                        "AS "
                        f"SELECT DISTINCT * FROM `{PROJECT_ID}.{DATASET}.{TABLE}`",
                "useLegacySql": False,
            }
        },
    )

    excel_to_json() >> json_to_bq() >> delete_duplicate

# =================== DAG =======================

pipeline = taskflow()

# =================== END =======================
