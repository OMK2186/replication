import yaml
import airflow
from airflow import DAG
from datetime import datetime, timedelta, time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
import pendulum
from airflow.providers.slack.hooks.slack import BaseHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

from copy import deepcopy

from airflow.providers.postgres.hooks.postgres import PostgresHook

import logging as log

# slack_msg = "YOUR DAG IS FAILED"
# SLACK_CONN_ID = 'slack'

# slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
# channel =  BaseHook.get_connection(SLACK_CONN_ID).login;

import boto3
import uuid
from pyathena import connect

# @author ketankk

s3_staging_dir = "s3://mbk-athena-temp/Unsaved/replication"
region_name = "ap-south-1"
boto3.setup_default_session(region_name=region_name)

athena_conn = connect(s3_staging_dir=s3_staging_dir, region_name=region_name)

s3 = boto3.resource('s3')

args = {
    'owner': 'dataengg',
    'email': 'dataengg@mobikwik.com',
    'email_on_failure': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=30),
}

local_tz = pendulum.timezone("Asia/Kolkata")

dag = DAG(
    dag_id='20230601_pg_merchant_replication',
    default_args=args,
    schedule_interval='30 7 * * *',
    start_date=datetime(2023, 6, 1, tzinfo=local_tz),
    # dagrun_timeout=timedelta(minutes=300),
    tags=['replication', 'pgsql', 'merchant'],
    catchup=False
)

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

import boto3
import json
from pyathena import connect


# TODO should be set at table level

# s3://mbk-nifi-landingzone/maxdelta/altce/allindiastatecodedata/allindiastatecodedata.txt

def get_max_delta(landing_bucket, max_delta, tgt_db, tgt_table):
    s3 = boto3.client('s3')
    key = f"{max_delta}/{tgt_db}/{tgt_table}/{tgt_table}.txt"
    obj = s3.get_object(Bucket=landing_bucket, Key=key)
    max_time = json.loads(obj['Body'].read())['lastModified']

    return max_time


##queryStart = client.start_query_execution(QueryString = 'SELECT * FROM myTable',QueryExecutionContext = {'Database': 'myDatabase'}, ResultConfiguration = { 'OutputLocation': 's3://your-bucket/key'})


# queryStart = client.start_query_execution(QueryString = 'SELECT * FROM mobinew.member limit 10', ResultConfiguration = {'OutputLocation': s3_staging_dir})
# results = client.get_query_results(QueryExecutionId=queryStart['QueryExecutionId'])


# TODO in case of CDC, use min of ds-4 and max_time.

def fetch_data(ex_query, src_table_name, columns, audit_col, max_time, curr_time):
    pgsql_hook = PostgresHook(postgres_conn_id=pgsql_merchant_conn, schema=src_db_name)

    query1 = ""
    df1 = pgsql_hook.get_pandas_df(sql=query1)
    print(df1)


# Default LIMIT is 200000, table specific LIMIT can be passed in parameter
def fetch_full_data2(pgsql_merchant_conn, src_db_name, src_table_name, columns, p_keys, s3_out_loc, CHUNKSIZE=200000):
    '''  pgsql_merchant = PostgresHook(postgres_conn_id=pgsql_merchant_conn, schema=src_db_name)#.get_sqlalchemy_engine()

    count_query = f"select count(*) as total_count from {src_table_name}"
    total_count = pgsql_hook.get_pandas_df(sql=count_query)['total_count'][0]
    log.info(f"Total count is: {total_count}")

    #   batch_size = total_count / LIMIT
    #   batch_num = 0
    #    OFFSET = 0

    engine = pgsql_hook
    conn = engine.connect().execution_options(stream_results=True)
    import pandas.io.sql as psql

    ext_query = f"select {columns} from {src_table_name} ORDER BY {p_keys} OFFSET {OFFSET} LIMIT {LIMIT}"

    for data_df in psql.read_sql(ext_query, con=conn, chunksize=LIMIT):
        for df in psql.read_sql(query1, con=conn, chunksize=200000):
            print(i)
            i = i + 1

        uid = str(uuid.uuid1())
        log.info(f"Data fetched: {data.shape}")

        file_path = f"{s3_out_loc}/{uid}.parquet"
        log.info(f"Writing data to {file_path}")

        data.to_parquet(file_path, index=False)

        OFFSET = OFFSET + LIMIT
        batch_num = batch_num + 1
        '''


def fetch_full_data(pgsql_merchant_conn, src_db_name, src_table_name, columns, p_keys, s3_out_loc, CHUNKSIZE=200000):
    pgsql_merchant = PostgresHook(postgres_conn_id=pgsql_merchant_conn, schema=src_db_name).get_sqlalchemy_engine()

    # Using stream_results=True to keep cursor at server side
    # Reference https://github.com/pandas-dev/pandas/issues/12265

    conn = pgsql_merchant.connect().execution_options(stream_results=True)
    import pandas.io.sql as psql

    ext_query = f"select {columns} from {src_table_name}"
    log.info(f"Running query: {ext_query}")

    for data_df in psql.read_sql(ext_query, con=conn, chunksize=CHUNKSIZE):
        uid = str(uuid.uuid1())
        log.info(f"Data fetched: {data_df.shape}")

        file_path = f"{s3_out_loc}/{uid}.parquet"
        log.info(f"Writing data to {file_path}")

        data_df.to_parquet(file_path, index=False)


def extract_load_data(params, ds):
    log.info(f"Parameters are: {params}")
    pgsql_merchant_conn = 'pgsql_150.24_merchant'

    landing_bucket = params.get('landing_bucket')
    data_path = params.get('data_path')
    max_delta = params.get('max_delta')

    src_db_name = params.get('src_db_name')
    src_table_name = params.get('src_table_name')
    columns = params.get('columns')
    load_type = params.get('load_type')
    audit_col = params.get('audit_col')
    primary_keys = params.get('primary_keys')

    target_db_name = params.get('target_db_name')
    target_table_name = params.get('target_table_name')

    ##max_time = get_max_delta(landing_bucket, max_delta, tgt_db, tgt_table)
    # TODO max_time for CDC / FULLLOAD / INCREMENTAL
    s3_out_loc = f"s3://{landing_bucket}/{data_path}/{target_db_name}/{target_table_name}"
    # s3://mbk-data-users/KK/personalization/kk_user_personalization_all/20230217_124530_00060_nnmee_06005b59-b1d3-4c94-a6a9-5b7356f29ac0

    if load_type == 'FULL':
        ex_query = "select {columns} from {table} "
        # fetch_full_data(src_db_name, src_table_name, columns, primary_keys):
        fetch_full_data(pgsql_merchant_conn, src_db_name, src_table_name, columns, primary_keys, s3_out_loc,
                        CHUNKSIZE=200000)

        # TODO
    elif load_type == 'CDC':
        ex_query = "select {columns} from {table}  where {audit_col} >='{st_date}' and {audit_col} <'{end_date}'"
        #max_time = get_max_delta(landing_bucket, max_delta, target_db_name, tgt_table)
    elif load_type == 'INCREMENTAL':
        ex_query = "select {columns} from {table}  where {audit_col} >='{st_date}' and {audit_col} <'{end_date}'"
    else:
        log.error(f"{load_type} is not a valid load_type. Supported types are FULL, CDC, INCREMENTAL")
        raise Exception(f"{load_type} is not a valid load_type. Supported types are FULL, CDC, INCREMENTAL")

    # fetch_data(ex_query, src_table_name, columns, audit_col, max_time, ds)


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


# TODO

# bulk_table_list=['partnertransactiondetails']


with open('/apps/cron/aws_replication/metadata/pg_merchant_replication.yaml') as f:
    configFile = yaml.safe_load(f)

    # Extract table names and fields to be processed
    tables = configFile['tables']

    # Common parameters for extraction
    src_db_af_conn_id = configFile['src_db_af_conn_id']
    landing_bucket = configFile['landing_bucket']
    data_path = configFile['data_path']
    max_delta = configFile['max_delta']
    columns = configFile['columns']

    # Common parameters for Glue Job
    glue_job_name = configFile['glue_job_name']  # Common Glue job, If other job needs to be used, add in table params
    target_bucket = configFile['target_bucket']  # mbk-datalake-common-prod'

    #########TODO Make required changes....

    # In this loop tasks are created for each table defined in the YAML file
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

            # If some parameter is specified for ant table use that otherwise use default params
            if params.get('conn_id') is None or params.get('conn_id') == '':
                ext_params['conn_id'] = src_db_af_conn_id

            if params.get('landing_bucket') is None or params.get('landing_bucket') == '':
                ext_params['landing_bucket'] = landing_bucket

            if params.get('data_path') is None or params.get('data_path') == '':
                ext_params['data_path'] = data_path

            if params.get('columns') is None or params.get('columns') == '':
                ext_params['columns'] = columns

            # 1.  First step is to start extraction using python pandas
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
