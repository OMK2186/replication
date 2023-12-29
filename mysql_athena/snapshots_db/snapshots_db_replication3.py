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
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


# TODO
def get_max_time(target_db_name, target_table_name):
    return datetime.now() - timedelta(days=10)


uq_id = str(uuid.uuid1().hex[0:12])

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
    dag_id='snapshots_db_replication',
    default_args=args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=120),
    tags=['dataengg', 'mysql', 'athena', 'mobinew', 'snapshot', 'replication'],
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


def extract_load_data():
    print('extract_load_data')


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


def abc():
    submit_glue_job = GlueJobOperator(
        dag=dag,
        task_id="submit_glue_job",
        job_name="mbk_transform_updated",
        num_of_dpus=2,
        # verbose=True,
        script_args={"--table_metafile": "s3://mbk-nifi-landingzone/Glue/mobikwik_schema_a_mongo_upiloading_debit.txt"},
        #    script_location=f"s3://{bucket_name}/etl_script.py",
        #    s3_bucket=bucket_name,
        #   iam_role_name=role_name,
        # create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.2X"},
    )


import yaml
from copy import deepcopy

with open('/apps/cron/aws_replication/metadata/snapshots_db_replication.yaml') as f:
    configFile = yaml.safe_load(f)
    # TODO delete this
    print(configFile)
    tables = configFile['tables']

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

submit_glue_job = GlueJobOperator(
    dag=dag,
    task_id="submit_glue_job",
    job_name="mbk_transform_updated",
    num_of_dpus=2,
    # verbose=True,
    script_args={"--table_metafile": "s3://mbk-nifi-landingzone/Glue/mobikwik_schema_a_mongo_upiloading_debit.txt"},
    iam_role_name="AWS-DataEnggUser-RoleSwitch-Supernova",
    # create_job_kwargs={"GlueVersion": "3.0", "NumberOfWorkers": 2, "WorkerType": "G.2X"},
)

if __name__ == "__main__":
    dag.cli()

## classairflow.providers.amazon.aws.operators.glue.GlueJobOperator ====
# https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/operators/glue/index.html
