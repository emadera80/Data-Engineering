import datetime as dt 
from datetime import timedelta 
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator 
import pandas as pd 

def csvToJSON(): 
    df = pd.read_csv('/mnt/d/onedrive/datascience/data engineer course/projects/data-engineering/airflow csvtojson/data.csv')
    for i, r in df.iterrows(): 
        print(r['name'])
    
    df.to_json('/mnt/d/onedrive/datascience/data engineer course/projects/data-engineering/airflow csvtojson/fromAirflow.json', orient='records')

default_args = {
    'owner': 'mannymadera',
    'start_date': dt.datetime(2021, 1, 29),
    'retries': 1, 
    'retry_delay': dt.timedelta(minutes=5),

}

with DAG('MYCSVDAG', 
    default_args=default_args, 
    schedule_interval=timedelta(minutes=5), ) as dag: 

    print_starting = BashOperator(task_id ='starting', bash_command='echo "I am reading the csv now...."')

    csvJSON = PythonOperator(task_id = 'convertCSVtoJSon', python_callable=csvToJSON)

print_starting >> csvJSON