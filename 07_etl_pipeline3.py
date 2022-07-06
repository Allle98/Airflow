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

from utils._mytask import extract,transform,load

@dag(
    dag_id='07_etl_3test',
    schedule_interval="@once",#'@daily',
    default_args=default_args,
    catchup=False,
    tags=['iungo']
    )
def main():
    ''' test ETL '''
    order_data = extract()                           # extract
    order_summary = transform(order_data)            # transform
    load(order_summary["total_order_value"])         # load

# =================== DAG =======================

dag = main()

# ==================== END =========================
