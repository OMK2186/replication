__author__ = 'ketankk'

import logging as log
import uuid
from datetime import datetime, timedelta

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from pymongo import MongoClient


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



########### Glue JOb from Python

s3_staging_dir = "s3://mbk-athena-temp/Unsaved/behml"
region_name = "ap-south-1"
boto3.setup_default_session(region_name=region_name)

athena_conn = connect(s3_staging_dir=s3_staging_dir, region_name=region_name)

glue_client = boto3.client('glue', region_name="ap-south-1")


s3_staging_dir = "s3://mbk-athena-temp/Unsaved/replication"
region_name = "ap-south-1"
boto3.setup_default_session(region_name=region_name)

athena_conn = connect(s3_staging_dir=s3_staging_dir, region_name=region_name)

glue_client = boto3.client('glue', region_name="ap-south-1")

double_col_list = ["amount_paid"]
ts_col_list = ["booked_at", "debited_at", "created_at", "last_modified"]
params = {'double_col_list': double_col_list, 'ts_col_list': ts_col_list}

'''
{
  "_id": "POLMBK7DDGH4C0E",
  "_class": "com.mobikwik.crosssell.insurance.policy.entities.PolicyEntity",
  "insuranceId": "INSE0D668J30H205F",
  "memberId": "9804526853@nocash.mobikwik.com",
  "primaryCell": "9804526853",
  "walletTransactionId": "586848618",
  "crossSell": false,
  "client": "android",
  "ip": "42.110.148.13",
  "insuranceSellPlatform": "ADD_MONEY_CROSS_SELL",
  "insurer": "ONEASSIST",
  "status": "PURCHASED",
  "insuranceCategory": "WALLET_PROTECT",
  "amountPaid": 79,
  "sumAssured": "150000",
  "sumAssuredUnit": "",
  "userAppDetails": {
    "userClientType": "Android",
    "subClientType": "Invalid",
    "appVersion": "1490"
  },
  "tenureInMonths": 1,
  "masterPolicyNo": "",
  "masterCode": "",
  "kyc": false,
  "bookedAt": "2020-10-29T08:53:44.805Z",
  "debitedAt": "2020-10-29T08:53:21.701Z",
  "validFrom": "2020-10-28T18:30:00Z",
  "validUpto": "2020-11-28T18:29:59.999Z",
  "policyDispatchTime": "2020-10-29T08:53:44.808Z",
  "policyDocUrl": "https://promotions.mobikwik.com/inapp/terms-and-conditions/wallet-assist/files/wallet-assist-tnc.pdf",
  "policyref": "1007925644",
  "customerDetails": {
    "kycType": "E-aadhaar xml Kyc",
    "name": "Abhishek Kumar Singh",
    "gender": "Male",
    "genderE": "MALE",
    "dob": "31-01-1985",
    "email": "9804526853@nocash.mobikwik.com",
    "mobile": "9804526853",
    "address": "BHARPARA ROAD 34/35 Haora (M.Corp) Howrah West Bengal 711103",
    "state": "West Bengal",
    "pincode": "711103",
    "nomineeName": "",
    "nomineeAge": "",
    "nomineeGender": "",
    "nomineeRelationship": "",
    "additionalInfo": {}
  },
  "wapgTxnInfo": {
    "pgStatus": false,
    "pgAmount": 0,
    "agId": "INS-POLMBK7DDGH4C0E"
  },
  "policyIconUrl": "https://static.mobikwik.com/appdata/revamp/insurance/oneassist-wp-policyIcon.png",
  "responseType": "INCOMPLETE_DETAILS",
  "sent": true,
  "purchaseResponse": {
    "customerId": "11836001",
    "memUUID": "1a0e45f0-6243-475a-a92d-566002324ef6",
    "message": "CUSTOMER_CREATED_SUCCESSFULLY",
    "code": "success"
  },
  "autoRenew": false,
  "autoPurchase": false,
  "userChoseToSkipNominee": false,
  "userCrossSellRandomizationId": "5f5e4172c9e77c003983f669",
  "notificationData": {
    "smsCount": 0,
    "appNotificationCount": 0,
    "whatsAppNotificationCount": 0,
    "totalNotifications": 0,
    "notificationRecords": []
  },
  "errorDetailsList": [],
  "formVersion": "Old Form",
  "source": "API",
  "version": 8,
  "createdAt": "2020-10-29T08:53:21.245Z",
  "lastModified": "2020-10-29T08:53:44.808Z"
}
'''

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


#############

# mongo_athena_mobikwik_kyc_data.py


def get_mongo_connection(_mongo_host, _mongo_db, _mongo_port, _mongo_user, _mongo_password):
    try:
        if _mongo_host and _mongo_db and _mongo_port and _mongo_user and _mongo_password:
            client = MongoClient(_mongo_host, _mongo_port, username=_mongo_user, password=_mongo_password,
                                 authSource=_mongo_db)
            log.info(f"Server info is {client.server_info()}")
            db_conn = client[_mongo_db]
            return db_conn
            # _coll = db[_collection]
    except Exception as e:
        log.error(f"Error while connecting to Mongo Server: {_mongo_host}, DB: {_mongo_db}, UserName: {_mongo_user}")
        log.error(f"Error is: {e}")
        raise


def save_data(df, s3_loc):
    uq_id = str(uuid.uuid4())
    landing_data_path = f"{s3_loc}/{uq_id}.parquet"
    print(f"landing_data_path: {landing_data_path}")
    df.to_parquet(landing_data_path, index=False)


def data_prep(df):
    df['updated_at'].fillna('2001-01-01', inplace=True)
    df['created_at'].fillna('2001-01-01', inplace=True)
    # TODO Changes required here;
    # Removing Millisecs from datetime
    df['updated_at'] = df['updated_at'].astype('datetime64[s]').astype('str')
    df['created_at'] = df['created_at'].astype('datetime64[s]').astype('str')
    df['obj_id'] = df['obj_id'].astype('str')
    # df.drop('_id', axis=1, inplace=True)
    return df

def read_data2():
    from pyspark.sql import SparkSession, SQLContext
    from pyspark import SparkConf, SparkContext
    sc = SparkContext()
    spark = SparkSession(sc)
    mongo_host: "10.10.150.147"
    mongo_port: 27017
    mongo_db: "mg_cross_sell"
    mongo_user: "analytics-app"
    mongo_pass: "Analytics@890#31090"
    mongo_coll="policy"

#df = pd.DataFrame({'a': pd.Series(dtype='int'), 'b': pd.Series(dtype='str'), 'c': pd.Series(dtype='float')})
    cols=['obj_id', 'member_id', 'insurance_category', 'insurance_sell_platform', 'insurer', 'amount_paid', 'cross_sell', 'booked_at', 'debited_at', 'created_at', 'last_modified']
    cols = {'obj_id': pd.Series(dtype='str'),
            'member_id': pd.Series(dtype='str'),
            'insurance_category': pd.Series(dtype='str'),
            'insurance_sell_platform': pd.Series(dtype='str'),
            'insurer': pd.Series(dtype='str'),
            'amount_paid': pd.Series(dtype='float64'),
            'cross_sell': pd.Series(dtype='bool'),
            'booked_at': pd.Series(dtype='datetime64'),
            'debited_at': pd.Series(dtype='datetime64'),
            'created_at': pd.Series(dtype='datetime64'),
            'last_modified': pd.Series(dtype='datetime64')}
    df = pd.DataFrame(arr, columns=cols)

    conn_url=f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:27017/{mongo_db}.{mongo_coll}?authSource={mongo_db}"
    data = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.uri", conn_url).load()

# monthspaidfor,status,sumassured,tenureinmonths,validfrom,validupto,policyref, customerdetails.address, gender, city , dob

# status, sumAssured, tenureInMonths, validFrom, validUpto, policyref, "customerDetails": {"state": "West Bengal","pincode": "711103",},
# , , , , , , "customerDetails": {"state": "West Bengal","pincode": "711103",},


def extract_policy_data(src_coll, load_type, audit_col, exec_day, s3_loc):
    # Need these columns, projection used for creating aliases
    proj = {"$project": {
        "obj_id": "$_id",
        "member_id": "$memberId",
        "insurance_category": "$insuranceCategory",
        "insurance_sell_platform": "$insuranceSellPlatform",
        "insurer": "$insurer",
        "amount_paid": "$amountPaid",
        "cross_sell": "$crossSell",
        "booked_at": "$bookedAt",  ##**
        "status": "$status",
        "sum_assured": "$sumAssured",
        "tenure_months": "$tenureInMonths",
        "policy_ref": "$policyref",
        "customer_state": "$customerDetails.state",
        "customer_pincode": "$customerDetails.pincode",
        "valid_from": "$validFrom",
        "valid_upto": "$validUpto",
        "debited_at": "$debitedAt",
        "created_at": "$createdAt",
        "last_modified": "$lastModified"}}

    # TODO
    if load_type == 'FULL':
        print(f"Load type: {load_type}")

    # TODO check max, current etcs
    elif load_type == 'CDC':
        # max_time = get_max_delta(landing_bucket, max_delta, target_db_name, tgt_table)
        max_time = datetime.now() - timedelta(days=10)  # TODO change this
        # If data has not been updated for long time, then take date till which data has been updated
        start_day = min(exec_day, max_time) - timedelta(days=5)
        end_day = exec_day + timedelta(days=1)

    elif load_type == 'INCREMENTAL':
        print(f"Load type: {load_type}")

    print(f"Date range is: {start_day} to {end_day}")
    sd = start_day
    ed = start_day + timedelta(days=1)

    while sd < end_day:
        query = {"$match": {audit_col: {"$gte": sd, "$lt": ed}}}
        pipeline = [query, proj]
        log.info(f"Query is: {query}")
        mongo_data = src_coll.aggregate(pipeline)
        arr = []
        for data in mongo_data:
            arr.append(data)
        df = pd.DataFrame(arr)
        print(f"Data Extracted for sd:{sd}, ed:{ed}, Row count is {df.shape[0]}")
        # TODO Check this df = data_prep(df)
        save_data(df, s3_loc)
        sd = ed
        ed = ed + timedelta(days=1)

# TODO
def get_max_time(target_db_name, target_table_name):
    return datetime.now() - timedelta(days=10)


def extract_load_data(params, ds):
    db_conn = get_mongo_connection(mongo_host, mongo_db, mongo_port, mongo_user, mongo_pass)

    log.info(f"Parameters are: {params}")

    landing_bucket = params.get('landing_bucket')
    data_path = params.get('data_path')
    # max_delta = params.get('max_delta')

    src_db_name = params.get('src_db_name')
    src_table_name = params.get('src_table_name')
    load_type = params.get('load_type')
    audit_col = params.get('audit_col')
    primary_keys = params.get('primary_keys')

    target_db_name = params.get('target_db_name')
    target_table_name = params.get('target_table_name')
    max_time = get_max_time(target_db_name, target_table_name)

    ##max_time = get_max_delta(landing_bucket, max_delta, tgt_db, tgt_table)
    # TODO max_time for CDC / FULLLOAD / INCREMENTAL

    s3_out_loc = f"s3://{landing_bucket}/{data_path}/{target_db_name}/{target_table_name}"
    # s3://mbk-data-users/KK/personalization/kk_user_personalization_all/20230217_124530_00060_nnmee_06005b59-b1d3-4c94-a6a9-5b7356f29ac0

    exec_day = datetime.strptime(ds, '%Y-%m-%d')

    if src_table_name == "policy":
        extract_policy_data(db_conn[src_table_name], load_type, audit_col, exec_day, s3_out_loc)

    elif src_table_name == "dummy2":
        print(f"Create extract function for this table: {src_table_name}")

    if load_type == 'FULL':
        print(f"{load_type}")

    # fetch_full_data(src_db_name, src_table_name, columns, primary_keys):
    #  fetch_full_data(pgsql_merchant_conn, src_db_name, src_table_name, columns, primary_keys, s3_out_loc, CHUNKSIZE=200000)

    # TODO
    elif load_type == 'CDC':
        ex_query = "select {columns} from {table}  where {audit_col} >='{st_date}' and {audit_col} <'{end_date}'"
        # max_time = get_max_delta(landing_bucket, max_delta, target_db_name, tgt_table)
    elif load_type == 'INCREMENTAL':
        ex_query = "select {columns} from {table}  where {audit_col} >='{st_date}' and {audit_col} <'{end_date}'"
    else:
        log.error(f"{load_type} is not a valid load_type. Supported types are FULL, CDC, INCREMENTAL")
        raise Exception(f"{load_type} is not a valid load_type. Supported types are FULL, CDC, INCREMENTAL")

    # fetch_data(ex_query, src_table_name, columns, audit_col, max_time, ds)


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
    dag_id='mg_cross-sell_replication',
    default_args=args,
    schedule_interval='55 6 * * *',
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=120),
    tags=['dataengg', 'mongo', 'athena', 'mg_cross_sell', 'replication'],
    catchup=False
)


def glue_job_task(task_name, params, pool):
    ex_task = PythonOperator(
        task_id=f'{task_name}-glue_job',
        python_callable=glue_job,
        dag=dag,
        op_kwargs={'params': params},
        provide_context=True,
        pool=pool
    )
    return ex_task


start = DummyOperator(
    task_id='start',
    dag=dag
)

validation_report = BashOperator(
    task_id='validation_report',
    bash_command="echo 'Hello validation..'",
    dag=dag)

end = DummyOperator(
    task_id='end',
    dag=dag)


def glue_job(params):
    print(params)


def extract_data_task(task_name, params, pool):
    ex_task = PythonOperator(
        task_id=f'{task_name}-ext_job',
        python_callable=extract_load_data,
        dag=dag,
        op_kwargs={'params': params},
        provide_context=True,
        pool=pool
    )
    return ex_task


import yaml
from copy import deepcopy

with open('/apps/cron/aws_replication/metadata/mg_kyc_replication.yaml') as f:
    configFile = yaml.safe_load(f)
    # TODO delete this
    print(configFile)
    tables = configFile['tables']

    mongo_host = configFile["mongo_host"]
    mongo_db = configFile["mongo_db"]
    mongo_port = configFile["mongo_port"]
    mongo_user = configFile["mongo_user"]
    mongo_pass = configFile["mongo_pass"]

    landing_bucket = configFile['landing_bucket']
    data_path = configFile['data_path']
    max_delta = configFile['max_delta']
    columns = configFile['columns']

    # Common parameters for Glue Job
    glue_job_name = configFile['glue_job_name']  # Common Glue job, If other job needs to be used, add in table params
    target_bucket = configFile['target_bucket']  # mbk-datalake-common-prod'

    for table in tables:
        for table, params in table.items():

            src_table = params.get('src_table_name')
            tgt_table = params.get('target_table_name')

            if src_table != tgt_table:
                task_name = f"{params.get('src_table_name')}-{params.get('target_table_name')}"
            else:
                task_name = src_table

            ext_params = deepcopy(params)
            glue_params = deepcopy(params)

            # 1.  First step is to start extraction using python pandas
            if params.get('landing_bucket') is None or params.get('landing_bucket') == '':
                ext_params['landing_bucket'] = landing_bucket

            if params.get('data_path') is None or params.get('data_path') == '':
                ext_params['data_path'] = data_path
            start_ext_job = extract_data_task(task_name, ext_params, 'replication_ext_pool')

            # 2. Second step is to start glue job
            if params.get('glue_job_name') is None or params.get('glue_job_name') == '':
                glue_params['glue_job_name'] = glue_job_name

            if params.get('target_bucket') is None or params.get('target_bucket') == '':
                glue_params['target_bucket'] = target_bucket

            glue_pool = 'glue_pool'

            start_glue_job = glue_job_task(f'{task_name}', glue_params, glue_pool)

            start >> start_ext_job
            start_ext_job >> start_glue_job
            start_glue_job >> validation_report >> end

if __name__ == "__main__":
    dag.cli()
