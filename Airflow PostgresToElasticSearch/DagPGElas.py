import datetime as dt 
from datetime import timedelta 

from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.python_operator import PythonOperator

import pandas as pd 
import psycopg2 as pg 
from elasticsearch import Elasticsearch


def queryPostgres(): 
    conn = pg.connect("host=127.0.0.1 port=5432 dbname=maderaanalytics user=manny password=Yankees1")
    df=pd.read_sql("select name, city from users", conn)
    df.to_csv('/mnt/c/Users/Enmanuel Madera/OneDrive/DataScience/Data Engineer Course/Projects/Data-Engineering/Airflow PostgresToElasticsearch/postgresdata.csv')
    print("------------Data Saved----------------")

def insertElastic(): 
    es = Elasticsearch()
    df = pd.read_csv('/mnt/c/Users/Enmanuel Madera/OneDrive/DataScience/Data Engineer Course/Projects/Data-Engineering/Airflow PostgresToElasticsearch/postgresdata.csv')
    for i, r in df.iterrows(): 
        doc = r.to_json()
        res = es.index(index="fromPostgres", doc_type="doc", body=doc)
        print(res)

default_args = {
    'owner': 'mannymadera',
    'start_date': dt.datetime(2021,1,30),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

with DAG('PGtoElastic', 
        default_args=default_args, 
        schedule_interval=timedelta(minutes=5), 
        ) as dag: 
    getData = PythonOperator(task_id='QueryPostgres', python_callable=queryPostgres)

    insertData = PythonOperator(task_id='insertElastic', python_callable=insertElastic)

getData >> insertData