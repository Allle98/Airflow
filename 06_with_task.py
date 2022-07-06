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

@dag(
    dag_id='06_task_dag',
    schedule_interval="@once",#'@daily',
    default_args=default_args,
    catchup=False,
    tags=['iungo']
    )
def main():
    '''test extract bitcoin price '''
    
    @task(task_id='extract', retries=2)
    def extract_bitcoin_price() -> Dict[str, float]:
        return requests.get(API_get_url).json()['bitcoin']

    @task
    def process_data(response: Dict[str, float]) -> Dict[str, float]:
        logging.info(response)
        return {'usd': response['usd'], 'change': response['usd_24h_change']}

    @task
    def store_data(data: Dict[str, float]):
        logging.info(f"Store: {data['usd']} with change {data['change']}")
        print(f"Store: {data['usd']} with change {data['change']}")

    store_data(process_data(extract_bitcoin_price()))

# =================== DAG =======================

dag = main()

# ==================== END =========================

'''
The resulting DAG has much less code and is easier to read. Notice that it also doesn’t require using ti.xcom_pull
and ti.xcom_push to pass data between tasks. This is all handled by the TaskFlow API when we define our task dependencies
with store_data(process_data(extract_bitcoin_price())). '''