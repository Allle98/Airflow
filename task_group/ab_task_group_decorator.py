import os
from socket import timeout
import pandas as pd
import datetime
import pendulum

from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
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

# =================== DAGS =======================

# pipeline setup
@dag(dag_id="ab_task_group_decorator", schedule_interval="@once", default_args=default_args, catchup=False, tags=['iungo', 'task_group'])
def main():

    @task
    def init():
        return 0

    @task_group(group_id='group_1')
    def group_1(value):

        # The @tasks below can be defined outside function group_1
        # What matters is where they are referenced
        @task(task_id='subtask_1')
        def task_1(value):
            task_1_result = value + 1
            return task_1_result

        @task(task_id='subtask_2')
        def task_2(value):
            task_2_result = value + 2
            return task_2_result

        @task(task_id='subtask_3')
        def task_3(value):
            task_3_result = value + 3
            return task_3_result

        # tasks are referenced here
        task_1_res = task_1(value)
        task_2_res = task_2(value)
        task_3_res = task_3(value)

        # sending this list to group_2
        return [task_1_res, task_2_res, task_3_res]

    @task_group(group_id='group_2')
    def group_2(list):
        @task(task_id='subtask_4')
        def task_4(values):
            return sum(values)

        @task(task_id='subtask_5')
        def task_5(value):
            return value*2

        # task_4 will sum the values of the list sent by group_1
        # task_5 will multiply it by two.
        task_5_result = task_5(task_4(list))

        return task_5_result

    @task
    def end(value):
        print(f'End! Value: {value}')

    end(group_2((group_1(init()))))

# =================== DAG =======================

taskflow = main()

# =================== END =======================


