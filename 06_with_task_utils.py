from airflow.decorators import dag, task
import pendulum
import datetime
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

from utils._bitcoin import extract_bitcoin_price,process_data,store_data

@dag(
    dag_id='06_task_dag_utils',
    schedule_interval="@once",#'@daily',
    default_args=default_args,
    catchup=False,
    tags=['iungo']
    )
def main():
    '''test extract bitcoin price '''

    store_data(process_data(extract_bitcoin_price(API_get_url)))

# =================== DAG =======================

dag = main()

# ==================== END =========================

'''
The resulting DAG has much less code and is easier to read. Notice that it also doesn’t require using ti.xcom_pull
and ti.xcom_push to pass data between tasks. This is all handled by the TaskFlow API when we define our task dependencies
with store_data(process_data(extract_bitcoin_price())). '''