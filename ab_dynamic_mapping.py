import os
from socket import timeout
import pandas as pd
import datetime
import pendulum

from airflow.decorators import dag, task, task_group

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

# =================== DAGS =======================

# pipeline setup
@dag(dag_id="ab_dynamic_mapping", schedule_interval="@once", default_args=default_args, catchup=False, tags=['iungo', 'task_group'])
def main():

    @task
    def init():
        return 0


    @task(task_id="task_function")
    def task_function(value: int, add: int):
        result_task = value + add
        return result_task


    @task_group(group_id='group_2')
    def group_2(list):
        @task(task_id='subtask_1')
        def task_4(values):
            return sum(values)

        @task(task_id='subtask_2')
        def task_5(value):
            return value*2

        # task_4 will sum the values of the list sent by group_1
        # task_5 will multiply it by two.
        task_5_result = task_5(task_4(list))

        return task_5_result

    @task
    def end(value):
        print(f'End! Value: {value}')

    num_task = 3
    result = task_function.partial(value=init()).expand(add=[*range(1, num_task + 1)])     # a volte non finisce il calcolo e passa ad eseguire il subtask_1 in group_2
    end(group_2(result))
    

# =================== DAG =======================

taskflow = main()

# =================== END =======================  