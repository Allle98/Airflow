from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pendulum
import datetime
import json
from typing import Dict
import requests
import logging

# ======================================================
default_args = {
                "owner": "D&G",
                "start_date": pendulum.datetime(2022, 5, 2),
                "retries": 1,
                "retry_delay": datetime.timedelta(minutes=60),
                'email_on_failure': False,
                }

# =================== FUNCTIONS =======================
## path = os.path.join(os.getcwd(), "dags/....)

API_get_url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"

def extract_bitcoin_price():
    return requests.get(API_get_url).json()['bitcoin']

def process_data(ti):
    response = ti.xcom_pull(task_ids='extract_bitcoin_price')
    logging.info(response)
    processed_data = {'usd': response['usd'], 'change': response['usd_24h_change']}
    ti.xcom_push(key='processed_data', value=processed_data)

def store_data(ti):
    data = ti.xcom_pull(task_ids='process_data', key='processed_data')
    logging.info(f"Store: {data['usd']} with change {data['change']}")
    print(f"Store: {data['usd']} with change {data['change']}")

# =================== DAGS =======================

# pipeline setup
dag = DAG(  
          dag_id='06_classic_dag', 
          schedule_interval="@once",#'@daily',
          default_args=default_args,
          catchup=False,
          tags=['iungo']
          )

extract_bitcoin_price = PythonOperator(
                                        task_id='extract_bitcoin_price',
                                        python_callable = extract_bitcoin_price,
                                        dag=dag
                                        )

process_data = PythonOperator(
                            task_id='process_data',
                            python_callable = process_data,
                            dag=dag
                            )

store_data = PythonOperator(
                            task_id='store_data',
                            python_callable = store_data,
                            dag=dag
                            )

# =================== SCHEMA =======================

extract_bitcoin_price >> process_data >> store_data

# ==================== END =========================