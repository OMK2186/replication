__author__ = 'ketankk'

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

from airflow.operators.python_operator import PythonOperator
import logging
import uuid
import mysql.connector as connection
import pandas as pd

log = logging.getLogger(__name__)
import json
import requests
from airflow.operators.python_operator import PythonOperator

local_tz = pendulum.timezone("Asia/Kolkata")
DAG_ID = 'replicationV2_mysql_mobinew'


# ################## [Start-Nifi Part] ######
# 1. Get the nifi metadata based on mapping_id
# 2. Execute Http Call
# 3. Inside NiFi, update status in the replication status table
def trigger_nifi(mapping_id):
    trigger_id = f"{str(uuid.uuid4().hex[:12])}_{mapping_id}"

    metadata_details = {'user_name': 'analytics',
                        'password': 'vsn@0pl3TYujk23(o',
                        'host_name': 'data-analytics-mysql-prod.mbkinternal.in',
                        'db_name': 'mobikwik_schema', }  # db_name='replication_metadata'}

    db_conn = connection.connect(host=metadata_details['host_name'],
                                 user=metadata_details['user_name'],
                                 password=metadata_details['password'],
                                 database=metadata_details['db_name'])
    nifi_query = f"SELECT '{trigger_id}' as trigger_id, {mapping_id} as mapping_id, nifi_conn_id, auto_incr_col, " \
                 f"audit_col_name, max_audit_val, nifi_target_bucket, batch_size, nifi_target_loc_prefix, " \
                 f"entity_src_schema, entity_src_name, entity_target_schema, entity_target_name FROM nifi_metadata nm " \
                 f"join entity_mapping em on em.mapping_id= nm.mapping_id WHERE nm.mapping_id={mapping_id}"

    nifi_metadata = pd.read_sql(nifi_query, db_conn).to_dict('records')[0]
    nifi_metadata_req_body = json.dumps(nifi_metadata)

    # nifi_metdata = pd.read_sql(nifi_query, db_conn).to_json(orient='records')[0]

    log.info(f"Nifi metadata: {nifi_metadata_req_body}")

    '''nifi_metadata = {
        "trigger_id": trigger_id,
        "nifi_conn_id": nifi_conn_id,
        "src_schema_name": src_schema_name,
        "src_table_name": src_table_name,
        "target_schema_name": target_schema_name,
        "target_table_name": target_table_name,
        "auto_incr_col": auto_incr_col,
        "audit_col_name": audit_col_name,
        "max_audit_val": max_audit_val,
        "batch_size": batch_size,

    }'''

    '''nifi_metadata={"trigger_id": "b2d92f10f95f_1", "mapping_id": 1, "nifi_conn_id": "mysql_an_250",  
    "audit_col_name": "updatedat", "max_audit_val": "2001-01-01", "default_new_audit_val":"2023-01-01",
    nifi_target_bucket": "mbk-nifi-landingzone", "batch_size": 100000, "nifi_target_loc_prefix": 
    "replication_v2/data", "entity_src_schema": "mobinew", "entity_src_name": "memberdetail", "entity_target_schema": 
    "mobinew", "entity_target_name": "memberdetail"} nifi_metadata_req_body = json.dumps(nifi_metadata)'''

    # [2023-10-12 21:16:01,257] {<stdin>:1} INFO - Nifi metadata: {"trigger_id": "959cb132b17a_1", "mapping_id": 1,
    # "nifi_conn_id": "mysql_an_250", "auto_incr_col": null, "audit_col_name": "updatedat", "max_audit_val":
    # "2001-01-01", "nifi_target_bucket": "mbk-nifi-landingzone", "batch_size": 100000, "nifi_target_loc_prefix":
    # "replication_v2/data", "entity_src_schema": "mobinew", "entity_src_name": "memberdetail",
    # "entity_target_schema": "mobinew", "entity_target_name": "memberdetail"}

    nifi_url = 'http://localhost:8083/replicationV2'
    #requests.post(nifi_url, json=nifi_metadata_req_body)
# use this to pass data as josn, json= takes data with escape char
    requests.post(nifi_url, data=nifi_metadata_req_body)


# ############# [END-Nifi Part] ###########


# ################## [Start-GLUE Part] ######
# 1. Get the Glue metadata based on mapping_id
# 2. Execute AWSGlue Operator Call
# 3. Override the Glue operator and update the status in the replication status table
def trigger_glue(mapping_id):
    nifi_query = f"select nifi_conn_id, auto_incr_col, audit_col_name, max_audit_val, nifi_target_bucket, " \
                 f"nifi_target_loc_prefix where mapping_id={mapping_id}"

    # Replication Metadata, Nifi-metadata
    metadata_details = {'user_name': 'analytics',
                        'password': 'vsn@0pl3TYujk23(o',
                        'host_name': 'data-analytics-mysql-prod.mbkinternal.in',
                        'db_name': 'mobikwik_schema', }  # db_name='replication_metadata'}

    mydb = connection.connect(user=metadata_details['user_name'], password=metadata_details['password'], host=host_name,
                              database=db_name)
    query = "Select * from entity_mapping limit 10;"
    # query = "Select * from entity_list limit 10;"
    entity_mapping_list = pd.read_sql(query, mydb)


# ############# [END-Glue Part] ###########


slack_msg = "Hello from Airflow"
SLACK_CONN_ID = 'mbk_slack'

slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
channel = BaseHook.get_connection(SLACK_CONN_ID).login;


# https://hooks.slack.com/services/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
# https://hooks.slack.com/services/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
def slack_success_notification(context):
    slack_msg = """
            :large_green_circle: Paylater Replication Success.
            *Execution Time*: {exec_date}
            """.format(
        exec_date=datetime.now().strftime("%Y-%m-%d")  # context.get('execution_date'),
    )
    success_alert = SlackWebhookOperator(
        task_id='slack_success_notification',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg)
    return success_alert.execute(context=context)

'''
SlackWebhookHook(self.http_conn_id, self.webhook_token, self.message, self.attachments, self.blocks, self.channel,
                 self.username, self.icon_emoji, self.icon_url, self.link_names, self.proxy)
SlackWebhookHook(http_conn_id=SLACK_CONN_ID, webhook_token=slack_webhook_token, message=slack_msg,
                 channel='#data_engineering', username='airflow')
'''

def slack_notification(context):
    slack_msg = """
            :red_circle: Task Failed.
            *OWNER*: {owner}
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        owner=args['owner']
        # log_url=context.get('task_instance').log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg)
    return failed_alert.execute(context=context)


args = {
    'owner': 'dataengg',
    'email': 'dataengg@mobikwik.com',
    'email_on_failure': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=20),
    'on_retry_callback': slack_notification
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval=None,
    start_date=datetime(2021, 4, 19, tzinfo=local_tz),
    # dagrun_timeout=timedelta(minutes=300),
    tags=['replication-v2', 'mobinew'],
    catchup=False
)


# [START]
# DAG listing
def glue_job(mapping_id):
    print(mapping_id)
def nifi_job(mapping_id):
    print(mapping_id)


def create_glue_task(task_id, mapping_id):
    return PythonOperator(task_id=f'{task_id}-{mapping_id}-glue',
                               python_callable=glue_job,
                               dag=dag,
                               op_kwargs={'mapping_id': mapping_id},
                               provide_context=True, )



def create_nifi_task(task_id, mapping_id):
    return PythonOperator(task_id=f'{task_id}-{mapping_id}-nifi',
                               python_callable=trigger_nifi,
                               dag=dag,
                               op_kwargs={'mapping_id': mapping_id},
                               provide_context=True, )


start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag, )


import mysql.connector as connection
import pandas as pd

# Replication Metadata
rep_md_details = {'user_name': 'analytics',
                  'password': 'vsn@0pl3TYujk23(o',
                  'host_name': 'data-analytics-mysql-prod.mbkinternal.in',
                  'db_name': 'mobikwik_schema'}
# db_name='replication_metadata'

rep_md_conn = connection.connect(user=rep_md_details['user_name'],
                                 password=rep_md_details['password'],
                                 host=rep_md_details['host_name'],
                                 database=rep_md_details['db_name'])

query = f"SELECT mapping_id, entity_src_name, entity_target_name FROM entity_mapping WHERE af_dag_id='{DAG_ID}'"
entity_mapping_list = pd.read_sql(query, rep_md_conn)

# Listing all the tables here having mapping as this {DAG_ID}

for index, row in entity_mapping_list.iterrows():
    # print(row[index])
    mapping_id = row['mapping_id']
    entity_src_name = row['entity_src_name']
    entity_target_name = row['entity_target_name']
    nifi_task = create_nifi_task(f'{entity_src_name}-{entity_target_name}', mapping_id)
    glue_task = create_glue_task(f'{entity_src_name}-{entity_target_name}', mapping_id)
    start >> nifi_task >> glue_task >> end

# [END]
# https://hooks.slack.com/services/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
