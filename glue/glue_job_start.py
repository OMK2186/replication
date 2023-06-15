import boto3
import time


glue_client = boto3.client('glue', region_name="ap-south-1")


target_db="paylater"
target_table="userpaylaterbalance_closing"
metafile=f"s3://mbk-nifi-landingzone/replication_v2/configs/{target_db}/{target_table}.glue.csv"

args={'--type':'1', '--y_day': '20230401', '--d_day':  '20230420', '--table_metafile':  metafile}

response = glue_client.start_job_run(
               JobName = 'rep_mbk_snapshot',
               Arguments = args )




def check_status(job_name, run_id):
  while True:
    status_response = glue_client.get_job_run(JobName=job_name, RunId=run_id)
    run_status=status_response['JobRun']['JobRunState']
    print(run_status)
    time.sleep(5)
    if run_status == 'FAILED':
      print('Job Failed..:(')
      break
    if run_status == 'SUCCEEDED':
      print('Job Successful..!!')
      break
  return run_status







from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import pendulum
from datetime import datetime, timedelta, time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.bash import BashOperator
import pendulum

import logging



from pymongo import MongoClient
from datetime import date, timedelta
import pandas as pd
import dateutil.parser

import logging
import pandas as pd
import pymysql
from urllib.parse import quote
import csv
import collections.abc

from datetime import datetime

#from mongo_extractor import MongoExtractor


from airflow import DAG
from airflow.operators.python_operator import PythonOperator

#from airflow.operators import S3KeySensor
#from airflow.operators.sensors import S3KeySensor

from airflow.providers.amazon.aws.sensors.s3_key import S3KeySensor
    

#TODO change email here..
#TODO Change retries..
args = {
    'owner': 'dataengg',
    'email': 'ketan.keshri@mobikwik.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

local_tz = pendulum.timezone("Asia/Kolkata")

dag = DAG(
    dag_id='20230421_replication_snapshot',
    default_args=args,
    schedule_interval='20 1 * * *',
    start_date=datetime(2022,11,1,tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=120),
    tags=['replication','snapshot'],
    catchup=False
    )

load_data = BashOperator(
    task_id='load_data',
    bash_command='echo {{ds}}',
    dag=dag
)

wallet_var4_2_task = PythonOperator(
    task_id='wallet_var4_2',
    python_callable=wallet_var4_2,
    dag=dag,
    provide_context=True,
)




sensor >> load_data

if __name__ == "__main__":
    dag.cli()
