from datetime import datetime
import os
import pandas as pd
import requests
#import gcsfs

from airflow.decorators import dag
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator

DATASET = "online_retail_ds"
TABLE = "online_retail"
OUTPUT_FILE = 'from_bq_online_retail.csv'
BUCKET_NAME = "test_airflow_infomanager"

@dag(dag_id='ab_bq_to_bucket', schedule_interval='@once', start_date=datetime(2022, 1, 1), tags=['iungo', 'gcs', 'bucket'], catchup=False)
def taskflow():

    bq_to_gcs = BigQueryToCloudStorageOperator(
        task_id="bq_to_gcs",
        source_project_dataset_table=f"{DATASET}.{TABLE}",
        destination_cloud_storage_uris=[f"gs://{BUCKET_NAME}/{OUTPUT_FILE}"],
        export_format="csv",
        field_delimiter=",",
        print_header=True,
    )
    bq_to_gcs

graph = taskflow()