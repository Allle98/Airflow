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
class Employee:
    """A sample Employee class"""

    def __init__(self, first, last, pay):
        self.first = first
        self.last = last
        self.pay = pay

    @property
    def email(self):
        return '{}.{}@email.com'.format(self.first, self.last)

    @property
    def fullname(self):
        return '{} {}'.format(self.first, self.last)

    def __repr__(self):
        return "Employee('{}', '{}', {})".format(self.first, self.last, self.pay)

import sqlite3


# pipeline setup
@dag(
    dag_id="ab_sqlite_db", 
    schedule_interval="@once",#'@daily',
    default_args=default_args,
    catchup=False,
    tags=['iungo', 'sqlite']
)
def main():
    REL_PATH = os.path.join(os.getcwd(), "dags/")
    FILE_PATH = REL_PATH + "sqlite/"
    conn = sqlite3.connect(FILE_PATH + 'test.db')

    c = conn.cursor()
    def insert_emp(emp):
        with conn:
            c.execute("INSERT INTO employees VALUES (:first, :last, :pay)", {'first': emp.first, 'last': emp.last, 'pay': emp.pay})


    def get_emps_by_name(lastname):
        c.execute("SELECT * FROM employees WHERE last=:last", {'last': lastname})
        return c.fetchall()


    def update_pay(emp, pay):
        with conn:
            c.execute("""UPDATE employees SET pay = :pay
                        WHERE first = :first AND last = :last""",
                    {'first': emp.first, 'last': emp.last, 'pay': pay})


    def remove_emp(emp):
        with conn:
            c.execute("DELETE from employees WHERE first = :first AND last = :last",
                    {'first': emp.first, 'last': emp.last})


    @task
    def fun():
        
        
        c.execute("""CREATE TABLE IF NOT EXISTS employees (
                    first text,
                    last text,
                    pay integer
                    )""")
        c.execute("""DELETE FROM employees;""")            
        emp_1 = Employee('John', 'Doe', 80000)
        emp_2 = Employee('Jane', 'Doe', 90000)

        insert_emp(emp_1)
        insert_emp(emp_2)

        emps = get_emps_by_name('Doe')
        print(emps)

        update_pay(emp_2, 95000)
        #remove_emp(emp_1)

        emps = get_emps_by_name('Doe')
        print(emps)

    
    fun()

# =================== DAG =======================

taskflow = main()

# =================== END =======================
