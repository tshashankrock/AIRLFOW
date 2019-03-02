from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime,timedelta


default_args = {
    'owner': 'shashank',
    'start_date': datetime(2019,2,28),
    'email' : ['shashankt423@gmail.com'],
    'email_on_failure': False,
    'email_on_retry':False,
    'retries':2
}

dag = DAG('hello_world', description='First Program DAG',
schedule_interval="0 12 * * *", default_args= default_args)

t1 = BashOperator(
    task_id = 'printdate',
    bash_command = 'date',
    dag = dag)

t2 = BashOperator(
    task_id = 'sleep',
    bash_command='sleep 5',
    retries = 3,
    dag =dag)

templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ params.parameter }}"
    {% endfor %}
"""

t3 = BashOperator(
    task_id = 'templated',
    bash_command = templated_command,
    params={'parameter':'Hello check the parameter i passed in'},
    dag = dag
)

#                    _____sleep(t2)
#t2->sleep           |
#t1->printable       t1
#t3->bashoperator    |_____templated(t3)
#t2.set_upstream(t1)
#t3.set_upstream(t1)

t1.set_downstream(t2)
t3.set_downstream(t2)

# t2 >> t3 >> t1