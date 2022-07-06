from datetime import datetime
import os
import pandas as pd

from airflow import DAG
from airflow.decorators import dag
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator, BigQueryGetDatasetOperator, BigQueryGetDatasetTablesOperator
from airflow.operators.python import PythonOperator

@dag(dag_id='ab_bucket_test', schedule_interval='@once', start_date=datetime(2021, 1, 1), tags=['iungo', 'gcs'], catchup=False)
def taskflow():

    def how_many_tables(ti):
        #task_instance = kwargs['get_dataset_table']
        r = ti.xcom_pull(task_ids='get_dataset_table')
        print(f"Numero di tabelle in  {r[0]['datasetId']} è {len(r)}")
        print(type(r))

        # convert into DataFrame
        df = pd.DataFrame(r)
        print(df)
        print(type(df))

        print(df['datasetId'])
        print(df['datasetId'][0])
        

    # GCS_Files = GoogleCloudStorageListOperator(
    #     task_id='GCS_Files',
    #     bucket='sage-ship-351812',
    #     gcp_conn_id='google_cloud'
    # )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id='thgh',
        table_id="test_table",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )   
    
    get_dataset = BigQueryGetDatasetOperator(
        task_id="get_dataset",
        dataset_id='thgh',
    )

    get_dataset_table = BigQueryGetDatasetTablesOperator(
        task_id="get_dataset_table",
        dataset_id='thgh',
    )

    how_many_tables = PythonOperator(
        task_id="how_many_tables",
        python_callable=how_many_tables,
    )

    create_table >> get_dataset >> get_dataset_table >> how_many_tables

graph = taskflow()
