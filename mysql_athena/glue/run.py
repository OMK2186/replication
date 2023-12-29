__author__ = 'ketankk'

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

target_db = "paylater"
target_table = "userpaylaterbalance_closing"

metafile = f"s3://mbk-nifi-landingzone/replication_v2/configs/{target_db}/{target_table}.glue.csv"
job_name = 'rep_mbk_snapshot'


def run_glue_job(day):
    job_name = 'rep_mbk_snapshot'
    args = {'--type': '1', '--y_day': '20230401', '--d_day': day, '--table_metafile': metafile}
    print(args)
    response = glue_client.start_job_run(JobName=job_name, Arguments=args)
    print(response)
    return response['JobRunId']


def check_status(job_name, run_id):
    while True:
        status_response = glue_client.get_job_run(JobName=job_name, RunId=run_id)
        run_status = status_response['JobRun']['JobRunState']
        print(run_status)

        if run_status == 'FAILED':
            print('Job Failed..:(')
            break
        if run_status == 'SUCCEEDED':
            print('Job Successful..!!')
            break

        time.sleep(5)
    return run_status


def reload_partition(target_db, target_table):
    pd.read_sql(f"MSCK REPAIR TABLE {target_db}.{target_table}", athena_conn)


day = datetime.now()

for i in range(1, 3):
    run_day = (day - timedelta(i)).strftime('%Y%m%d')
    print(run_day)
    run_id = run_glue_job(run_day)
    run_status = check_status(job_name, run_id)
    if run_status != 'SUCCEEDED':
        print(f'Job Failed for {run_day}')

reload_partition(target_db, target_table)