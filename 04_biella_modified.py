import os
import pendulum
import datetime
import pandas as pd
from airflow import DAG

# Operators
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
#from airflow.operators.empty import EmptyOperator
#from airflow.exceptions import AirflowSensorTimeout
from airflow.operators.dummy import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.empty import EmptyOperator

# ===============================================
default_args = {
                "owner": "ab",
                "start_date": pendulum.datetime(2022, 5, 2),
                "retries": 1,
                "retry_delay": datetime.timedelta(minutes=60),
                "email": ["test@gmail.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                }

# =================== FUNCTIONS =======================
REL_PATH = os.path.join(os.getcwd(), "dags/") ############### os.sep
FILE_NAME = "online_retail.csv"
FILE_PATH = REL_PATH + "biella/" # + FILE_NAME


def save_data(df: pd.DataFrame, filename: str = "data.csv", data_path: str = FILE_PATH):
    data_path = data_path
    data_file = filename
    file_path = os.path.join(data_path, data_file)
    df.to_csv(file_path, index=False)
    return df.to_json()

def extract_data(url: str = "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"):
    df = pd.read_excel(url,nrows=100,)
    data_file = 'online_retail.csv'
    file_path = os.path.join(FILE_PATH, data_file)
    print(file_path)
    df.to_csv(file_path, index=False)
    return df.to_json()

# def extract_data(url: str = "https://archive.ics.uci.edu/ml/machine-learning-databases/00352/Online%20Retail.xlsx"):
#     print("QUI")
#     df = pd.read_excel(url,nrows=100,)
#     print("Fine")
#     df.to_csv(FILE_PATH, index=False)
#     return df.to_json()

def transform_data(filename: str = "online_retail.csv"):
    file_name = os.path.join(FILE_PATH, filename)
    df = pd.read_csv(file_name)
    df["TotalAmount"] = df["UnitPrice"] * df["Quantity"]
    df.drop(
        columns=["StockCode", "Description", "Country", "UnitPrice", "Quantity"],
        inplace=True,)
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"]).dt.date
    df["CustomerID"] = df["CustomerID"].astype("Int64")
    dfjson = save_data(df, filename='transformed.csv')
    return dfjson #json format

def group_data(filename: str = "transformed.csv"):
    file_name = os.path.join(FILE_PATH, filename)
    df = pd.read_csv(file_name)
    df_aggr = df.groupby(["InvoiceNo", "InvoiceDate", "CustomerID"])["TotalAmount"].sum()
    dfjson = save_data(df_aggr, filename="aggregated.csv")
    return dfjson

# =================== DAGS =======================

dag = DAG( dag_id="04_biella_modified", 
          schedule_interval="@once",#'@daily',
          default_args=default_args,
          catchup=False,
          tags=['iungo']
           )

# bash_extractor = BashOperator(
#                             task_id="bash_extractor",
#                             bash_command="python /opt/airflow/dags/04_biella.py extract_data",
#                             dag=dag,
#                             )

# python_extractor = PythonOperator(
#                                 task_id="python_extractor", 
#                                 python_callable=extract_data,
#                                 dag=dag,
#                                 )

python_transform = PythonOperator(
                                task_id="python_transform", 
                                python_callable=transform_data,
                                dag=dag,
                                )

python_group = PythonOperator(
                            task_id="python_group", 
                            python_callable=group_data,
                            dag=dag,
                            )


sensor_verify_csv= '/opt/airflow/dags/biella/*.csv'

sensor_file = FileSensor(
                        task_id="sensor_file",
                        filepath=sensor_verify_csv,
                        fs_conn_id='airflow_db',
                        dag=dag,
                        )

#  gcs_file_sensor_yesterday = GoogleCloudStorageObjectSensor(task_id='gcs_file_sensor_yesterday_task',bucket='myBucketName',object=full_path_yesterday)
#  #for this example we expect today not to exist, keep running until 120 timeout, checkout docs for more options like mode  and soft_fail
#  gcs_file_sensor_today = GoogleCloudStorageObjectSensor(task_id='gcs_file_sensor_today_task',bucket='myBucketName',object=full_path_today, timeout=120)



# start_op = DummyOperator(task_id="start_task",dag=dag)
# last_op = DummyOperator(task_id="last_task", dag=dag)

start_op = EmptyOperator(task_id="start_task",dag=dag)
last_op = EmptyOperator(task_id="last_task", dag=dag)

# =================== SCHEMA =======================

#start_op >>[bash_extractor,python_extractor ] >> python_transform >> sensor_file >> python_group >>last_op 
start_op >> sensor_file >> python_transform >> python_group >> last_op 
# ==================== END =========================

