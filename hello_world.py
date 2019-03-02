from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'start_date' : datetime(2019,2,28),
    'owner' : 'airflow',
    'catchup' : False        # minute,hour,day(month),month,day(week)
}


def print_hello():
    return 'Hello World!'

dag = DAG('hello_world', description='First Program DAG',
schedule_interval="0 12 * * *", default_args= default_args)



dummy_operator = DummyOperator(task_id='dummy_task',retries = 3,dag = dag)

hello_operator = PythonOperator(task_id = 'hello_task',python_callable = print_hello,dag=dag)

dummy_operator >> hello_operator