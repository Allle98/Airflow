import json
import pendulum
from airflow.decorators import dag, task
import datetime
import pendulum

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

@dag(
    dag_id='07_etl_dag',
    schedule_interval="@once",#'@daily',
    default_args=default_args,
    catchup=False,
    tags=['iungo']
    )
def taskflow_api_etl():

    @task
    def extract():
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        order_data_dict = json.loads(data_string)
        return order_data_dict

    @task(task_id="transform_task", multiple_outputs=True)
    def transform(order_data_dict: dict):
        total_order_value = 0
        for value in order_data_dict.values():
            total_order_value += value
        return {"total_order_value": total_order_value, "ciao": "prima", "ciao2": "seconda"}

    @task
    def load(total_order_value: float, e: str, c: str):
        print(f"Total order value is: {total_order_value:.2f}")
        print("Primo print ", e)
        print("Secondo print ", c)
    ############################################################################
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"], order_summary["ciao"], order_summary["ciao2"])

pippo = taskflow_api_etl()
