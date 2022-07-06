from datetime import datetime
import os
import pandas as pd
import requests

import pandas_gbq
from google.oauth2 import service_account
from google.cloud import bigquery

from airflow import DAG
from airflow.decorators import dag, task
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator



@dag(dag_id='ab_list_files', schedule_interval='@once', start_date=datetime(2022, 1, 1), tags=['iungo', 'gcs', 'bucket'], catchup=False)
def taskflow():
    
    gcs_file = GoogleCloudStorageListOperator(
        task_id='GCS_Files',
        bucket='test_airflow_infomanager',
        gcp_conn_id='google_cloud'
    )

graph = taskflow()