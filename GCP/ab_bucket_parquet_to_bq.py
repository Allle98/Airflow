from datetime import datetime
import os
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator


DATASET = "online_retail_ds"
TABLE = "online_retail_parquet"
DATA_FILE = "online_retail.csv"
PARQUET_FILE = "online_retail.parquet"
BUCKET_NAME = "test_airflow_infomanager"

@dag(dag_id='ab_bucket_parquet_to_bq', schedule_interval='@once', start_date=datetime(2022, 1, 1), tags=['iungo', 'gcs', 'bucket'], catchup=False)
def taskflow():
    
    # login = BashOperator(
    #     task_id="login",
    #     bash_command="gcloud auth activate-service-account --key-file=/opt/airflow/dags/credentials/iungo-infomanager.json"
    # )
    
    @task
    def to_parquet():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/opt/airflow/dags/credentials/iungo-infomanager.json"
  
        df = pd.read_csv(f"gs://{BUCKET_NAME}/{DATA_FILE}")
        df.to_parquet(f"gs://{BUCKET_NAME}/{PARQUET_FILE}")


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

    csv_to_bq = GCSToBigQueryOperator(
        task_id="parquet_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[PARQUET_FILE],
        destination_project_dataset_table=f"{DATASET}.{TABLE}",
        #skip_leading_rows=1,
        field_delimiter=',',
        source_format = 'parquet',
        write_disposition='WRITE_TRUNCATE',     # WRITE_TRUNCATE (sovrascrive), WRITE_APPEND o WRITE_EMPTY
    )

    to_parquet() >> create_empty_dataset >> create_empty_table >> csv_to_bq

graph = taskflow()