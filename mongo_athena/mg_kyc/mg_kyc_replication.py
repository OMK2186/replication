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


# mongo_athena_mobikwik_kyc_data.py
# Sample JSON


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
    # TODO CHECK engine here
    # df.to_parquet(landing_data_path, index=False, engine='fastparquet')
    df.to_parquet(landing_data_path, index=False)


# Collection Name: trackwizz_ckyc
'''
{
  "_id": "64be150503e8112fb17c96e2",
  "uuid": "601356c403beb",
  "requestId": "5fe678a0ac140",
  "encryptedPan": "Cadl9EAi0els9qQQvA/3cQ==",
  "searchRequest": {
    "InputIdType": "C",
    "InputIdNo": "AZJPA4191N",
    "RequestId": "99b88ad336db",
    "ParentCompany": "SC144"
  },
  "downloadRequest": {
    "RequestId": "601356c49f993",
    "ParentCompany": "SC144",
    "CKYCNumber": "60013050523574"
  },
  "createdAt": "2023-07-24T06:07:01.307Z",
  "updatedAt": "2023-07-24T06:07:03.374Z"
}
'''


def prep_trackwizz_ckyc_data(df):
    df['created_at'] = df.created_at.astype("datetime64[s]").where(df.created_at.notnull(), None)
    df['updated_at'] = df.updated_at.astype("datetime64[s]").where(df.updated_at.notnull(), None)
    df['obj_id'] = df.obj_id.astype("str")

    return df


def extract_trackwizz_ckyc_data(src_coll, load_type, audit_col, exec_day, s3_loc):
    # Need these columns, projection used for creating aliases

    print(src_coll, load_type, audit_col, exec_day, s3_loc)
    proj = {"$project": {
        "obj_id": "$_id",
        "uuid": "$uuid",
        "request_id": "$requestId",
        "encrypted_pan": "$encryptedPan",
        "sr_req_id": "$searchRequest.RequestId",
        "sr_input_id_type": "$searchRequest.InputIdType",
        "sr_input_id_num": "$searchRequest.InputIdNo",
        "sr_parent_company": "$searchRequest.ParentCompany",
        "dr_req_id": "$downloadRequest.RequestId",
        "dr_parent_company": "$downloadRequest.ParentCompany",
        "dr_ckyc_num": "$downloadRequest.CKYCNumber",
        "created_at": "$createdAt",
        "updated_at": "$updatedAt"}}
    # Creating DF with prdefined column names to handle blank data

    cols = {'obj_id': pd.Series(dtype='str'),
            'uuid': pd.Series(dtype='str'),
            'request_id': pd.Series(dtype='str'),
            'encrypted_pan': pd.Series(dtype='str'),
            'sr_req_id': pd.Series(dtype='str'),
            'sr_input_id_type': pd.Series(dtype='str'),
            'sr_input_id_num': pd.Series(dtype='str'),
            'sr_parent_company': pd.Series(dtype='str'),
            'dr_req_id': pd.Series(dtype='str'),
            'dr_parent_company': pd.Series(dtype='str'),
            'dr_ckyc_num': pd.Series(dtype='str'),
            'updated_at': pd.Series(dtype='datetime64[ns]'),
            'created_at': pd.Series(dtype='datetime64[ns]')}

    # TODO
    if load_type == 'FULL':
        print(f"Load type: {load_type}")

    # TODO check max, current etcs
    elif load_type == 'CDC':
        # max_time = get_max_delta(landing_bucket, max_delta, target_db_name, tgt_table)
        ##max_time = datetime.now() - timedelta(days=10)  # TODO change this
        max_time = datetime(2010, 4, 1)  # - timedelta(days=10)  # TODO change this
        # If data has not been updated for long time, then take date till which data has been updated
        # TODO Max date from some metadata file...
        start_day = min(exec_day, max_time) - timedelta(days=5)
        #start_day = exec_day - timedelta(days=10)
        end_day = exec_day + timedelta(days=2)

    #  TODO

    elif load_type == 'INCREMENTAL':
        print(f"Load type: {load_type}")

    print(f"Date range is: {start_day} to {end_day}")
    sd = start_day
    ed = datetime(2023, 1, 1)
    # ed = start_day + timedelta(days=1)

    while sd < end_day or False: # TODO remove this
        query = {"$match": {audit_col: {"$gte": sd, "$lt": ed}}}
        pipeline = [query, proj]
        log.info(f"Query is: {query}")
        mongo_data = src_coll.aggregate(pipeline)
        arr = []
        for data in mongo_data:
            arr.append(data)
        df = pd.DataFrame(arr, columns=cols)
        print(f"Data Extracted for sd:{sd}, ed:{ed}, Row count is {df.shape[0]}")
        # TODO Check this df = data_prep(df)
        if not df.empty:
            df = prep_trackwizz_ckyc_data(df)
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

    if src_table_name == "trackwizz_ckyc":
        extract_trackwizz_ckyc_data(db_conn[src_table_name], load_type, audit_col, exec_day, s3_out_loc)

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


# ####### [START] GLUE JOB Task CODE[START]

'''
{
  "double_col_list": [
    "amount_paid"
  ],
  "ts_col_list": [
    "booked_at",
    "valid_from",
    "valid_upto",
    "debited_at",
    "created_at",
    "last_modified"
  ],
  "landing_bucket": "mbk-nifi-landingzone",
  "data_path": "data",
  "target_bucket": "mbk-datalake-common-prod",
  "target_db_name": "mg_cross_sell",
  "target_table_name": "policy",
  "p_key": "obj_id",
  "audit_col": "last_modified",
  "insert_type": "upsert",
  "partition_src_col": "created_at",
  "partition_col": "day"
}
'''


def check_status(glue_client, job_name, run_id):
    import time
    while True:
        status_response = glue_client.get_job_run(JobName=job_name, RunId=run_id)
        run_status = status_response['JobRun']['JobRunState']
        print(run_status)
        time.sleep(5)

        if run_status == 'FAILED':
            print('Job Failed..:(')
            break
        if run_status == 'SUCCEEDED':
            print('Job Successful..!!')
            break
    return run_status


def trigger_glue_job(glue_client, job_name, glue_args):
    # args = {'--type': '1', '--y_day': '20230401', '--d_day': day, '--table_metafile': metafile}
    print(glue_args)
    response = glue_client.start_job_run(JobName=job_name, Arguments=glue_args)
    print(response)
    log.info(response)
    return response['JobRunId']


def glue_job():
    import boto3
    glue_client = boto3.client('glue', region_name="ap-south-1")
    # TODO job name to be passed from config..
    job_name = 'mongo_athena_hudi_job'
    run_id = trigger_glue_job(glue_client, job_name, glue_args)
    check_status(job_name)


# ########## [END] GLUE JOB CODE [END]

# #Airflow Code

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
    dag_id='mg_kyc_replication',
    default_args=args,
    schedule_interval='55 6 * * *',
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=120),
    tags=['dataengg', 'mongo', 'athena', 'kyc', 'replication'],
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
