import datetime as dt
import csv
import airflow
import requests
import os
from datetime import datetime
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json

 
dag = DAG(                                                     
   dag_id="MultiPath",                          
   schedule_interval="@daily",                                     
   start_date=dt.datetime(2023, 2, 28), 
   catchup=False,
)


P1 = BashOperator(
    task_id="P1",
    #bash_command=uniqDateCommand,
    bash_command="echo 'hello' > /opt/airflow/data/Staging/hello.txt",
    dag=dag,
)

P2 = BashOperator(
    task_id="P2",
    #bash_command=uniqDateCommand,
    bash_command="echo 'hello' > /opt/airflow/data/Staging/hello.txt",
    dag=dag,
)

P3 = BashOperator(
    task_id="P3",
    #bash_command=uniqDateCommand,
    bash_command="echo 'hello' > /opt/airflow/data/Staging/hello.txt",
    dag=dag,
)

P4 = BashOperator(
    task_id="P4",
    #bash_command=uniqDateCommand,
    bash_command="echo 'hello' > /opt/airflow/data/Staging/hello.txt",
    dag=dag,
)

P5 = BashOperator(
    task_id="P5",
    #bash_command=uniqDateCommand,
    bash_command="echo 'hello' > /opt/airflow/data/Staging/hello.txt",
    dag=dag,
)




P2.set_upstream(task_or_task_list=[P1])
P3.set_upstream(task_or_task_list=[P1])
P4.set_upstream(task_or_task_list=[P3])
P5.set_upstream(task_or_task_list=[P2])