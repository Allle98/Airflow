from datetime import datetime
import os
import requests

from airflow.decorators import dag
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator


API = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"
DATASET = "bitcoin_ds"
TABLE = "bitcoin"
FILE = os.getcwd() + os.sep + "dags" + os.sep + "GCS" + os.sep + "bitcoin.sql"        # percorso del file che verrà creato con la query di insert da eseguire

@dag(dag_id='ab_bq_bitcoin', schedule_interval='@once', start_date=datetime(2021, 1, 1), tags=['iungo', 'gcs'], catchup=False)
def taskflow():

    def extract_bitcoin_price():
        return requests.get(API).json()['bitcoin']


    def generate_query(ti):
        data = ti.xcom_pull(task_ids='extract_bitcoin_price')

        query = f"INSERT {DATASET}.{TABLE} VALUES ("
        for key in data.keys():
            if key == 'last_updated_at':
                query += "'" + str(datetime.fromtimestamp(data[key]).strftime('%Y-%m-%d %H:%M:%S')) + "'"
            else:
                query += str(data[key]) + ", "

        query += ");"

        with open(FILE, 'w') as file:
            file.write(query)
        file.close()


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
        schema_fields=[
            {'name': 'usd', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'usd_market_cap', 'type': 'DECIMAL', 'mode': 'NULLABLE'},
            {'name': 'usd_24_vol', 'type': 'DECIMAL', 'mode': 'NULLABLE'},
            {'name': 'usd_24_change', 'type': 'DECIMAL', 'mode': 'REQUIRED'},
            {'name': 'last_updated_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
        ],
        exists_ok=True, # If True, ignore “already exists” errors when creating the table.
    )

    extract_bitcoin_price = PythonOperator(
        task_id="extract_bitcoin_price",
        python_callable=extract_bitcoin_price
    )    

    generate_query = PythonOperator(
        task_id="generate_query",
        python_callable=generate_query
    ) 

    insert_value = BigQueryInsertJobOperator(
        task_id="insert_value",
        configuration={
            "query": {
                "query": open(FILE, 'r').read(),
                "useLegacySql": False,
            }
        },
    )

    create_empty_dataset >> create_empty_table >> extract_bitcoin_price >> generate_query >> insert_value

graph = taskflow()