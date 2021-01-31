import pandas as pd
import psycopg2
import os
from sqlalchemy import create_engine
from io import StringIO
import requests

import datetime as dt 
from datetime import timedelta 

from airflow import DAG 
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def getdata(): 
    url = 'https://data.wprdc.org/datastore/dump/5bbe6c55-bce6-4edb-9d04-68edeb6bf7b1'
    r = requests.post(url)
    if r.ok:
        data = r.content.decode('utf8')
        df = pd.read_csv(StringIO(data), skipinitialspace=True)
        df.to_csv('/mnt/d/OneDrive/DataScience/Data Engineer Course/Projects/Data-Engineering/Airflow PostgresToElasticsearch/data1.csv')

def cleanData(): 
    df = pd.read_csv('/mnt/d/OneDrive/DataScience/Data Engineer Course/Projects/Data-Engineering/Airflow PostgresToElasticsearch/data1.csv')
    final = df[['PROPERTYCITY', 'PROPERTYSTATE', 'PROPERTYZIP', 'MUNIDESC', 'SCHOOLDESC',  'SALEDATE','PRICE', 'SALECODE', 'SALEDESC', 'INSTRTYPDESC']]
    final['PRICE'] = final['PRICE'].fillna(0)
    final['PROPERTYZIP'] = final['PROPERTYZIP'].fillna(0)
    final.to_csv('/mnt/d/OneDrive/DataScience/Data Engineer Course/Projects/Data-Engineering/Airflow PostgresToElasticsearch/final.csv')

def loadData(): 
    final = pd.read_csv('/mnt/d/OneDrive/DataScience/Data Engineer Course/Projects/Data-Engineering/Airflow PostgresToElasticsearch/final.csv')
    # Initialize a string buffer
    sio = StringIO()
    sio.write(final.to_csv(index=None, header=None, sep='\t'))  # Write the Pandas DataFrame as a csv to the buffer
    sio.seek(0)  # Be sure to reset the position to the start of the stream

    engine = create_engine('postgres+psycopg2://user:pasword@127.0.0.1:5433/maderaanalytics')

    final.head(0).to_sql('alleghenyproperty', engine, if_exists='replace', index=False)

    conn=engine.raw_connection()
    cur = conn.cursor()
    cur.copy_from(sio, 'alleghenyproperty')
    conn.commit()

def deleteFiles(): 
    os.remove('/mnt/d/OneDrive/DataScience/Data Engineer Course/Projects/Data-Engineering/Airflow PostgresToElasticsearch/final.csv')
    os.remove('/mnt/d/OneDrive/DataScience/Data Engineer Course/Projects/Data-Engineering/Airflow PostgresToElasticsearch/data1.csv')

default_args = {
    'owner':'mannymadera', 
    'start_date': dt.datetime(2021,1,30),
    'retries': 1 ,
    'retry_delay': dt.timedelta(minutes=5),

}

with DAG ('loadAllyghennydata', 
    default_args=default_args , 
    schedule_interval=timedelta(minutes=5),) as dag: 

    getData = PythonOperator(task_id='GetData', python_callable=getdata)

    cleanData = PythonOperator(task_id='CleanData', python_callable=cleanData)

    loadData = PythonOperator(task_id='LoadData', python_callable=loadData)

    deletefile = PythonOperator(task_id='deletefile', python_callable=deleteFiles)

getData >> cleanData >> loadData >> deletefile

