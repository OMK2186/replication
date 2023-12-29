__author__ = 'ketankk'

import logging as log
import uuid
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import yaml
from copy import deepcopy


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

#Todo not needed, as directly using
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

# Todo Add time
dag = DAG(
    dag_id='snapshots_db_replication',
    default_args=args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=120),
    tags=['dataengg', 'mysql', 'athena', 'mobinew', 'snapshot', 'replication'],
    catchup=False
)

# TODO Get Custome Role created for this, and pass that as a param if supported on same EC2
def glue_job_task(task_name, glue_job_name):
    glue_task = GlueJobOperator(
        dag=dag,
        task_id=f'{task_name}-glue_job',
        job_name=glue_job_name,
        num_of_dpus=2,
        script_args={"--table_metafile": "s3://mbk-nifi-landingzone/Glue/mobikwik_schema_a_mongo_upiloading_debit.txt"},
        iam_role_name="AWS-DataEnggUser-RoleSwitch-Supernova",
    )
    return glue_task


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

import requests
# Trigger Nifi Job, through API Call
def extract_load_data(params):
    import requests
    log.info(f"Params: {params}")
    print(f"Params: {params}")
    print('extract_load_data')
    url = 'http://localhost:8001/contentListener'
    data = {"username": "xyz", "password": "xyz1", "param" : params}
    requests.post(url, json=data)


'''
import requests
url = 'http://localhost:8001/contentListener'
data = {"username":"xyz","password":"xyz"}
requests.post(url, json=data)

curl --request POST   --data '{"username":"xyz","password":"xyz","params":"234"}'  http://localhost:8001/contentListener 


'''

def extract_data_task(task_name, params):
    ex_task = PythonOperator(
        task_id=f'{task_name}-ext_job',
        python_callable=extract_load_data,
        dag=dag,
        op_kwargs={'params': params},
        provide_context=True,

    )
    return ex_task


'''
#Load types - [CDC, FULL, INCREMENTAL, ADHOC, SNAPSHOT_D, SNAPSHOT_W, SNAPSHOT_M, ]
#audit_col is null in case of FULL load, in case of INCREMENTAL it will be created_at*, CDC->updated_at*
#partition_col will be null if not partitioned, if there is created_at, it will date(created_at,'YYYYMMDD') format will be used
dag: 'snapshots_db_replication'
#src_db_af_conn_id: 'pgsql_150.24_merchant'
landing_bucket: 'mbk-nifi-landingzone' # Add this to particular table if you want to use diff landing_bucket for that table
data_path: 'data' # In this table extracted data will be stored. Add this to particular table if you want to use diff data_path for that table.
max_delta: 'maxdelta' #Location to store max_delta till which data has been extracted, This shoul be updated in metadata file
columns: '*' #column list in table if only particular columns needs to be extracted eg. columns:'memberid, txnid, createdat .. '
where: '1=1' #TODO check if this is necessary

#Config to be used for Glue Job
#glue_param_path:  's3://mbk-nifi-landingzone/Glue/' # not needed, will pass as a parameter directly
glue_job_name: '' #Common Glue job, If other job needs to be used, add in table params
target_bucket: 'mbk-datalake-common-prod'


tables:
    - snapshots_db_member_monthly:
        src_db_name: 'mobinew'
        src_table_name: 'member'
        load_type: 'SNAPSHOT_M'
        audit_col : 'updatedat'
        target_db_name: 'snapshots_db'
        target_table_name: 'member_monthly'
        primary_keys: 'id' #Comma separated values
        partition_col: 'day'
        partition_src_col: 'createdAt'


'''

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
        for table_name, params in table.items():

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

            # Nifi Job
            start_ext_job = extract_data_task(task_name, ext_params)

            # 2. Second step is to start glue job
            if params.get('glue_job_name') is None or params.get('glue_job_name') == '':
                glue_params['glue_job_name'] = glue_job_name

            if params.get('target_bucket') is None or params.get('target_bucket') == '':
                glue_params['target_bucket'] = target_bucket

            glue_pool = 'glue_pool'
            # glue_job_name = "mbk_transform_updated"

            start_glue_job = glue_job_task(f'{task_name}', glue_params)

            start >> start_ext_job
            start_ext_job >> start_glue_job
            start_glue_job >> validation_report >> end

'''
submit_glue_job_tmp = GlueJobOperator(
    dag=dag,
    task_id="submit_glue_job",
    job_name="mbk_transform_updated",
    num_of_dpus=2,
    script_args={"--table_metafile": "s3://mbk-nifi-landingzone/Glue/mobikwik_schema_a_mongo_upiloading_debit.txt"},
    iam_role_name="AWS-DataEnggUser-RoleSwitch-Supernova",
)
'''
##################
__author__ = 'ketankk'

import logging as log
import uuid
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import yaml
from copy import deepcopy


from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator


args = {
    'owner': 'dataengg',
    'email': 'dataengg@mobikwik.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=60)
}

local_tz = pendulum.timezone("Asia/Kolkata")

# Todo Add time
dag2 = DAG(
    dag_id='task_seq1',
    default_args=args,
    schedule_interval=None,
    start_date=datetime(2023, 1, 1, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=120),
    tags=['dataengg', 'mysql', 'athena', 'mobinew', 'snapshot', 'replication'],
    catchup=False
)



start = DummyOperator(
    task_id='start',
    dag=dag2
)

end = DummyOperator(
    task_id='end',
    dag=dag2
)
def create_athena_task(task_id, query):
    run_query = query
    task_id_name=task_id
    task = AWSAthenaOperator(
        task_id=task_id_name,
        query=run_query,
        database='mobikwik_schema',
        output_location='s3://mbk-athena-temp/athena-operator-testing/',
        sleep_time=5,
        max_tries=None,
        aws_conn_id='aws_default',
        dag=dag2)
    return task

def dyn_task(id):
    task = DummyOperator(task_id=f'task-{id}', dag=dag2)
    return task

queries={'q1':'1', 'q2':'2'}

prev_task = start
next_task = end
prev_task >> next_task


for key, val in queries.items():
    print(f"{key},{val}")
    task = DummyOperator(task_id=f'task-{key}', dag=dag2)
    prev_task >> task >> next_task
    next_task.downstream_task.remove(prev_task)
    prev_task = task



    #create_athena_task(f"task-{key}", val)


t1.downstream_task_ids.remove("t2")


if __name__ == "__main__":
    dag.cli()
