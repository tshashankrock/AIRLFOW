import psycopg2
import logging
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
global cur
global value
global tablelist
default_args = {
    'owner': 'CrispAnalytics',
    'start_date': datetime(2019,2,28),
    'email' : ['shashank.tripathi@lumiq.ai'],
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':2,
    'depends_on_past': False,
    'catchup' : False ,
  #  'scheduled_interval': '@daily'       # minute,hour,day(month),month,day(week)
}

def print_column(tablename):
    global cur
    #global value
    print("Inside print column")
    #value = kwargs["params"]["tablename"]
    #print(value)
    query = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = "+tablename+";"
    print(query)
    q1=cur.execute(query)
    print(q1)
    columns = [list(r) for r in cur.fetchall()]
    logging.info(columns)
    return columns        

dag =DAG('Assignment3',default_args=default_args,schedule_interval="0 12 * * *")


connection = BaseHook.get_connection("postgres")
conn = psycopg2.connect(host=connection.host,database=connection.schema,user = connection.login, password=connection.password, port=connection.port)
print(conn)
print("!!!!!!!!")
cur = conn.cursor()
print(cur)
cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema='public';""")
tables = [list(r) for r in cur.fetchall()]
tablelist = [item for sublist in tables for item in sublist]
dummy_operator = DummyOperator(task_id='dummy_task',retries = 3,dag = dag)
print(dummy_operator)
print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
for i in range(len(tablelist)):
    print(tablelist[i])
    val = tablelist[i]
    hello_operator=PythonOperator(
        task_id=tablelist[i],
        provide_context=True,
        python_callable=print_column,
        op_kwargs = {'tablename':val},
        dag = dag,
        )
    print(hello_operator)
    print("Hello above >>")
    hello_operator.set_upstream(dummy_operator)
    #dummy_operator.set_upstream(hello_operator)
    #dummy_operator >> hello_operator

#print("value of python_callable")
#print(value)
conn.commit()
cur.close()
conn.close()