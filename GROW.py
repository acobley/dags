import datetime as dt
import csv
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def Clean():
    print('Hello from clean')
    OutputFile=open('/tmp/Output.txt', 'w')
    OutputFile.write("Serial,Latitude,Longitude\n")
    OldSerial=""
    LineNumber=0
    with open('/tmp/data.csv', newline='') as csvfile:
       reader = csv.DictReader(csvfile)
       for row in reader:
          if OldSerial!=row['Serial'] :
             OldSerial=row['Serial']

             Serial=row['Serial']
             if len(Serial) > 10:
                Serial=Serial.split('.')[0]
              
             OutLine=Serial+","+row['Latitude']+","+row['Longitude']+"\n"
             print (LineNumber)
             LineNumber=LineNumber+1
             OutputFile.write(OutLine)

dag = DAG(                                                     
   dag_id="Process_GROW_Data",                          
   schedule_interval="@daily",                                     
   start_date=dt.datetime(2023, 2, 24), 
   catchup=False,
)
download_data = BashOperator(                              
   task_id="download",                               
   bash_command="curl -o /tmp/data.csv -L 'https://www.dropbox.com/s/y33jd7tscumu7hf/GrowLocations.csv?dl=0'",
   dag=dag,
)
transform = PythonOperator(
   task_id="transform",
   python_callable=Clean,
   dag=dag,
)

show = BashOperator(
    task_id="show",
    bash_command="wc /tmp/Output.txt",
    dag=dag,
)

copy=BashOperator(
    task_id="copy",
    bash_command="cp /tmp/Output.txt /opt/airflow/dags/web",
    dag=dag,
)

download_data >> transform >> show >> copy
    
