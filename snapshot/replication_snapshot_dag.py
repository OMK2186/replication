import yaml
import airflow
from airflow import DAG
from datetime import datetime, timedelta, time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
import pendulum

args = {
    'owner': 'dataengg',
    'email': 'dataengg@mobikwik.com',
    'email_on_failure': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


local_tz = pendulum.timezone("Asia/Kolkata")

dag = DAG(
    dag_id='20210312_memberbalance_replication',
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=datetime(2021, 6, 19, tzinfo=local_tz),
    #dagrun_timeout=timedelta(minutes=300),
    tags=['replication'],
    catchup=False
)

start = DummyOperator(
    task_id='start',
    dag=dag
)


def create_dynamic_task(task_id, command, pool):
    bash_command = command
    task = BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        dag=dag,
        pool=pool
    )
    return task

partition_refresh_memberbalanceupdated = BashOperator(
        task_id='partition_refresh_memberbalanceupdated',
        bash_command='python3 /apps/nifi/partition_refresh/partition_refresh_diff_location.py mobinew memberbalanceupdated s3://mbk-datalake-common-prod/mobinew/memberbalance/ ',
        dag=dag,
    )

end = DummyOperator(
    task_id='end',
    dag=dag)

with open('/apps/cron/aws_replication/metadata/memberbalance_replication.yaml') as f:
    configFile = yaml.safe_load(f)

    # Extract table names and fields to be processed
    tables = configFile['tables']

    # In this loop tasks are created for each table defined in the YAML file
    for table in tables:
        for table, fieldName in table.items():
            # First step is to start Nifi Job
            nifi_command = 'sh /apps/nifi/trigger_nifi_table.sh ' + fieldName.get("file") + ' '
            nifi_pool = 'nifi_pool'
            start_nifi_job = create_dynamic_task('{}-nifi_job'.format(fieldName.get("table_name")), nifi_command,
                                                 nifi_pool)

            # Second step is to start glue job
            if (fieldName.get('partitioned') == 'False'):
                pass
            else:
                glue_param_path = 's3://mbk-nifi-landingzone/Glue/' + fieldName.get("file")
                glue_command = 'sh /apps/nifi/memberbalance_glue_run.sh ' + glue_param_path + ' ' + fieldName.get(
                    "database_name") + ' ' + fieldName.get("table_name") + ' '
                glue_pool = 'glue_pool'
                start_glue_job = create_dynamic_task('{}-glue_job'.format(fieldName.get("table_name")), glue_command,
                                                     glue_pool)

            start >> start_nifi_job
            start_nifi_job >> start_glue_job
            start_glue_job >> end

end >> partition_refresh_memberbalanceupdated-