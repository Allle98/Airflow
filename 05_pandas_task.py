import os
from socket import timeout
import pandas as pd
import datetime
import pendulum 
from airflow.decorators import dag, task

from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.empty import EmptyOperator

# ===============================================
default_args = {
                "owner": "D&G",
                "start_date": pendulum.datetime(2022, 5, 2),
                "retries": 1,
                "retry_delay": datetime.timedelta(minutes=60),
                "email": ["test@gmail.com"],
                "email_on_failure": False,
                "email_on_retry": False,
                }

# =================== FUNCTIONS =======================
URL = "https://gist.githubusercontent.com/netj/8836201/raw/6f9306ad21398ea43cba4f7d537619d0e07d5ae3/iris.csv"
##22 mb, takes time to load

REL_PATH = os.path.join(os.getcwd(), "dags/")
FILE_PATH = REL_PATH + "out/"

# =================== DAGS =======================

# pipeline setup
@dag( dag_id="05_load_UCI_task", 
    schedule_interval="@once",#'@daily',
    default_args=default_args,
    catchup=False,
    tags=['iungo']
    )
def main():
    '''test do something '''

    @task(task_id='read_csv', retries=1)  # read
    def read_csv():
        df = pd.read_csv(URL, nrows=100)
        print(f"Total dataframe is {df}")
        return df.to_json()
    
    @task(task_id='save_data', retries=1)
    def save_data(df: pd.DataFrame, filename: str = "test.csv", data_path: str = FILE_PATH):
        # data_path = data_path
        data_file = filename
        file_path = os.path.join(data_path, data_file)
        df = pd.read_json(df)
        df.to_csv(file_path, index=False)
        print('saved!')
        return "done"

    # just to try, not a lot of sense here
    sensor_check_file = FileSensor(    
        task_id="sensor_check_file",
        poke_interval=1, # by default is 30 seconds
        fs_conn_id="file_connection",
        filepath=FILE_PATH,
        timeout=1,   # if we don't set a timeout my sensor will check every 30 (poke_interval) seconds during 7 days before timing out!!!
        #mode='reschedule',  # lets the other tasks work and then gets rescheduled, this to avoid deadlocks. Putting this is a best-practice
        #soft_fail=True # if my sensor is longer to execute than the time defined in timeout I want to skip the sensor
                        # if I don't want to fail. By default is set to False
    )
             
    fine = EmptyOperator(task_id="fine")
    #o = read_csv()
    
    sensor_check_file >> (save_data(read_csv())) >> fine
   


# =================== DAG =======================

taskflow = main()

# =================== END =======================

'''
The resulting DAG has much less code and is easier to read. Notice that it also doesn’t require using ti.xcom_pull
and ti.xcom_push to pass data between tasks. This is all handled by the TaskFlow API when we define our task dependencies'''
