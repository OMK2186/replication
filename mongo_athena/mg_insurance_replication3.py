#mongo --port 27017 -u analytics-app -p'Analytics@890#31090' --authenticationDatabase 'insurance' --host 10.10.150.147

import pandas as pd
import numpy as np
from datetime import timedelta,datetime
import sys
from dateutil import tz
import dateutil
from pymongo import MongoClient
import uuid


import yaml
import airflow
from airflow import DAG
from datetime import datetime, timedelta, time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
import pendulum


import boto3
import time
from datetime import datetime, timedelta
from pyathena import connect
import pandas as pd

s3_staging_dir = "s3://mbk-athena-temp/Unsaved/behml"
region_name = "ap-south-1"
boto3.setup_default_session(region_name=region_name)

athena_conn = connect(s3_staging_dir=s3_staging_dir, region_name=region_name)

glue_client = boto3.client('glue', region_name="ap-south-1")


audit_col='createdAt'


########## [ETL-END] #######


_mongo_host='10.10.150.147'
_mongo_user='analytics-app'
_mongo_password='Analytics@890#31090'
_mongo_port=27017
_collection='accountTransactions'
_mongo_db='insurance'


def get_mongo_connection():
    try:
        if _mongo_host and _mongo_db and _collection and _mongo_port and _mongo_user and _mongo_password:
            client=MongoClient(_mongo_host,_mongo_port,username=_mongo_user,password=_mongo_password,authSource=_mongo_db)
            db = client[_mongo_db]
            _coll=db[_collection]
            client.server_info()
    except Exception as e:
        print(e)
    return _coll

def get_mongo_data():
    _coll=get_mongo_connection()
    mongo_data=_coll.find()
    df=pd.DataFrame(mongo_data)
    print("data fetched")
    return df

def extract_data(ds):
    print("Execution date is: {}".format(ds))
    day=ds

    #audit column name
    #day="2023-02-21"
    end_day=datetime.strptime(day,'%Y-%m-%d') + timedelta(1)
    start_day=end_day-timedelta(5)
    print("Running for date>= {} and date<{}".format(start_day, end_day))

    audit_col = metadata['audit_col']
    df=fetch_data(audit_col, start_day, end_day)
    print("Data Extracted. Row count is {}".format(df.shape[0]))
    df=data_prep(df)
    print("Date cleaned and prepared, saving to the landing zone.")
    save_data(df)



def extract():
    print('ex')
 


def transform():
    print('tx')

 

def load():
    print('load')


def epfo_data():
    print('epfo')


########## [ETL-END] #######

######## [Glue start] ######

s3_staging_dir = "s3://mbk-athena-temp/Unsaved/replication"
region_name = "ap-south-1"
boto3.setup_default_session(region_name=region_name)

athena_conn = connect(s3_staging_dir=s3_staging_dir, region_name=region_name)

glue_client = boto3.client('glue', region_name="ap-south-1")

double_col_list = ["amount_paid"]
ts_col_list = ["booked_at", "debited_at", "created_at", "last_modified"]
params = {'double_col_list': double_col_list, 'ts_col_list': ts_col_list}



def trigger_glue_job(day):
  #job_name='rep_mbk_snapshot'
  #args={'--type':'1', '--y_day': '20230401', '--d_day':  day, '--table_metafile':  metafile}
  job_name = 'pg_merchant_tmp2'
  #args = {'--params':  params\'}
  args={}
  print(args)
  response = glue_client.start_job_run(JobName = job_name, Arguments = args)
  print(response)
  return response['JobRunId']


def check_status(job_name, run_id):
  while True:
    status_response = glue_client.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id)
    run_status=status_response['JobRun']['JobRunState']
    print(f'Job status: {run_status}')
    if run_status in ['SUCCEEDED', 'FAILED', 'ERROR', 'TIMEOUT', 'STOPPED']:
        break
    if run_status in ['RUNNING', 'WAITING', 'STOPPING', 'STARTING']:
        time.sleep(5)
    else:
        print(f'Unknown state: {run_status}')
        break
  return run_status


GLUE_JOB_NAME=''

def run_glue():
    print('Starting Glue Job...')
    run_id = trigger_glue_job()
    check_status(run_id)

######## [Glue end] ######



##Airflow Code

args = {
    'owner': 'dataengg',
    'email': 'dataengg@mobikwik.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=60)
}

local_tz = pendulum.timezone("Asia/Kolkata")

dag = DAG(
    dag_id='mg_insurance_replication',
    default_args=args,
    schedule_interval='20 9 * * *',
    start_date=datetime(2018,1,1,tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=120),
    tags=['dataengg','mongo','athena','replication','insurance',''],
    catchup=False
    )




start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)


table_list=['UserEpfoDetail','accountTransactions','accountStatements']

for table in table_list:

    extract_mongo_data = PythonOperator(task_id=f'mongo-etl-{table}', provide_context=True, python_callable=extract_data, dag=dag)
    start >> extract_mongo_data
    glue_job = PythonOperator(task_id=f'glue-{table}', provide_context=True, python_callable=run_glue, dag=dag)
    extract_mongo_data >> glue_job
    glue_job >> end


glue_load_refresh_data = BashOperator(
        task_id='glue_load_refresh_data',
        bash_command="sh /apps/nifi/glue_job_run.sh s3://mbk-nifi-landingzone/Glue/mg_mobikwik_onboarding_request.txt mg_mobikwik onboarding_request ",
        dag=dag
    )

#sh /apps/nifi/glue_job_run.sh s3://mbk-nifi-landingzone/Glue/mg_mobikwik_onboarding_request.txt mg_mobikwik onboarding_request



extract_mongo_data>>glue_load_refresh_data

if __name__ == "__main__":
    dag.cli()