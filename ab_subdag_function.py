import pendulum

from airflow import DAG
from airflow.example_dags.subdags.subdag import subdag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup



def subdag(parent_dag_name, child_dag_name, args):
    #with TaskGroup(group_id="group_1") as group_1:
    with DAG(dag_id='%s.%s' % (parent_dag_name, child_dag_name), default_args=args, schedule_interval="@once",) as dag_subdag:   # provare con i subtask
      
    #    @task(task_id='sub')
       
        def fun():
            print("ciao")


    
    for i in range(5):
        PythonOperator(
            task_id='%s-task-%s' % (child_dag_name, i + 1),
            python_callable=fun,
            default_args=args,
            dag=dag_subdag,
        )
    
    print("here")
    print(type(dag_subdag))
    return dag_subdag

args = {
        'owner': 'airflow',
        "start_date": pendulum.datetime(2022, 5, 2),
}

DAG_NAME = "ab_subdag_function"

dag = DAG(
    dag_id=DAG_NAME, 
    schedule_interval="@once",
    default_args=args,
    catchup=False,
    tags=['iungo', 'subdag']
)

start = EmptyOperator(
        task_id='start-of-main-job',
        dag=dag,
)

some_other_task = EmptyOperator(
        task_id='some-other-task',
        dag=dag,
)


end = EmptyOperator(
        task_id='end-of-main-job',
        dag=dag,
)

subdag = SubDagOperator(
        task_id='run-this-dag-after-previous-steps',
        subdag=subdag(DAG_NAME, 'run-this-dag-after-previous-steps', args),
        dag=dag,
)

start >> some_other_task >> end >> subdag