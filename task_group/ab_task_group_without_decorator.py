import os
from socket import timeout
import this
import pandas as pd
import datetime
import pendulum

from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator

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
@dag(dag_id="ab_task_group_without_decorator", schedule_interval="@once", default_args=default_args, catchup=False, tags=['iungo', 'task_group'])

def main():

    NUM_TASK_GROUP_1 = 3


    def init(**context):
        context['ti'].xcom_push(key='value', value=0)
        
    def end(**context):
        result = context.get('ti').xcom_pull(key='result')
        print(f"Result: {result}")


    init = PythonOperator(task_id="init", python_callable=init)
    end = PythonOperator(task_id="end", python_callable=end)
    
    # start definition first group of task
    with TaskGroup(group_id="group_1") as group_1:

        # =========== group_1 function ===========
        # same function for each subtask in group_1
        def add_value(**context):
            # get parameters
            value = context.get('ti').xcom_pull(key='value')
            num = context['add']
            
            # do stuff
            result = int(value) + int(num)

            # store result
            context['ti'].xcom_push(key=f'value_{num}', value=str(result))

        # ======== end group_1 function ==========

        for i in range(1, (NUM_TASK_GROUP_1 + 1)):
            t = PythonOperator(task_id=f"sub_task{i}", python_callable=add_value, op_kwargs={"add": i})
    # end definition first group of task
   
    # start definition second group of task
    with TaskGroup(group_id="group_2") as group_2:

        # =========== group_2 function ===========

        def sum_function(num_task=NUM_TASK_GROUP_1, **context):
            sum = 0
            for i in range(1, (NUM_TASK_GROUP_1 + 1)):
                value = context.get('ti').xcom_pull(key=f'value_{i}')
                sum += int(value)

            context['ti'].xcom_push(key='sum', value=sum)


        def multiply_by_two(**context):
            result = int(context.get('ti').xcom_pull(key='sum')) * 2
            context['ti'].xcom_push(key='result', value=result)

        # ======== end group_2 function ==========


        subtask_1 = PythonOperator(task_id="subtask_1", python_callable=sum_function)
        subtask_2 = PythonOperator(task_id="subtask_2", python_callable=multiply_by_two)
    
        subtask_1 >> subtask_2
    # end definition first group of task
    
    init >> group_1 >> group_2 >> end
    
# =================== DAG =======================

taskflow = main()

# =================== END =======================