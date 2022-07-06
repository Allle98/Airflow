from datetime import datetime
import os
import pandas as pd
import requests
#import gcsfs

from airflow.decorators import dag
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator

DATASET = "online_retail_ds"
TABLE = "online_retail"
DATA_FILE = 'online_retail.csv'
BUCKET_NAME = "test_airflow_infomanager"

@dag(dag_id='ab_bucket_to_bq', schedule_interval='@once', start_date=datetime(2022, 1, 1), tags=['iungo', 'gcs', 'bucket'], catchup=False)
def taskflow():

    create_empty_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_empty_dataset",
        dataset_id=DATASET,
        gcp_conn_id='google_cloud_default',
        exists_ok=True, # If True, ignore “already exists” errors when creating the table.
    )

    create_empty_table = BigQueryCreateEmptyTableOperator(
        task_id="create_empty_table",
        dataset_id=DATASET,
        table_id=TABLE,
        bigquery_conn_id='google_cloud_default',
        exists_ok=True, # If True, ignore “already exists” errors when creating the table.
    )

    # def fun():
    #     fs = gcsfs.GCSFileSystem(project=BUCKET_NAME)
    #     with fs.open(f"{BUCKET_NAME}/{DATA_FILE}") as f:
    #         df = pd.read_csv(f, nrows=1)
    #     return list(df)

    csv_to_bq = GCSToBigQueryOperator(
        task_id="csv_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[DATA_FILE],
        destination_project_dataset_table=f"{DATASET}.{TABLE}",
        skip_leading_rows=1,
        field_delimiter=',',
        source_format = 'csv',
        write_disposition='WRITE_TRUNCATE',     # WRITE_TRUNCATE (sovrascrive), WRITE_APPEND o WRITE_EMPTY
        # labels=pd.read_csv(f"gs://{BUCKET_NAME}/{DATA_FILE}", nrows=1),  # problemi
    )

    create_empty_dataset >> create_empty_table >> csv_to_bq

graph = taskflow()