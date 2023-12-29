__author__ = 'ketankk'

import uuid
from datetime import datetime, timedelta

import pandas as pd
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from pymongo import MongoClient

# mongo_athena_mobikwik_onboarding_request

_mongo_host = '10.10.150.149'
_mongo_user = 'analytics'
_mongo_password = '3Fgs6&hs1cvbHjkk'
_mongo_port = 27017
_collection = 'kyc_data'
_mongo_db = 'mobikwik'

metadata = {
    "schema": "mg_mobikwik",
    "table": "kyc_data",
    "audit_col": "updatedAt",
    "bucket": "mbk-nifi-landingzone",
    "datapath": "/data/"}


def get_mongo_connection():
    try:
        if _mongo_host and _mongo_db and _collection and _mongo_port and _mongo_user and _mongo_password:
            client = MongoClient(_mongo_host, _mongo_port, username=_mongo_user, password=_mongo_password,
                                 authSource=_mongo_db)
            db = client[_mongo_db]
            _coll = db[_collection]
            client.server_info()
    except Exception as e:
        print(e)
    return _coll


def save_data(df):
    data_path = "s3://" + metadata['bucket'] + metadata['datapath'] + "mg_" + _mongo_db + "/" + _collection + "/"
    uq_id = str(uuid.uuid4())
    data_path = data_path + uq_id + ".parquet"
    print("Saving File to location: {}".format(data_path))
    df.to_parquet(data_path, index=False)


def data_prep(df):
    df['requested_at'].fillna('2001-01-01', inplace=True)
    df['accepted_at'].fillna('2001-01-01', inplace=True)
    df['updated_at'].fillna('2001-01-01', inplace=True)
    df['created_at'].fillna('2001-01-01', inplace=True)
    # TODO Changes required here;
    # Removing Millisecs from datetime
    df['updated_at'] = df['updated_at'].astype('datetime64[s]').astype('str')
    df['created_at'] = df['created_at'].astype('datetime64[s]').astype('str')
    df['obj_id'] = df['obj_id'].astype('str')
    df['requested_at'] = df['requested_at'].astype('datetime64[s]').astype('str')
    df['accepted_at'] = df['accepted_at'].astype('datetime64[s]').astype('str')
    # df.drop('_id', axis=1, inplace=True)
    return df


"""CREATE EXTERNAL TABLE `mg_mobikwik.kyc_data`(
  `obj_id` string, 
  `full_name` string, 
  `member_id` string, 
  `contact_no` string, 
  `city` string, 
  `state` string, 
  `kycrequest_status` string, 
  `v4_errors` array<string>,
`long` double,
`lat` double,
`kyc_source` string,
`bank_acc_num` string,
`bank_ifsc` string,
`brand_name` string,
`permanent_addr` string,
`requested_at` timestamp,
`accepted_at` timestamp,
  `created_at` timestamp, 
  `updated_at` timestamp)
PARTITIONED BY ( 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mbk-datalake-common-prod/mg_mobinew/kyc_data'
"""


# permanentAddr, brandName, bankDetails.accountNumber, bankDetails.ifsc, kycSource, kycRequestStatus, acceptedAt,requestedAt, longitude, latitude
def fetch_data(audit_col, start_day, end_day):
    _coll = get_mongo_connection()
    # Need these columns, projection used for creating aliases
    # TODO Changes required here;
    proj = {"$project": {
        "obj_id": "$_id",
        "full_name": "$fullName",
        "member_id": "$memberId",
        "contact_no": "$contactNo",
        "city": "$city",
        "state": "$state",
        "long": "$longitude",
        "lat": "$latitude",
        "kycrequest_status": "$kycRequestStatus",
        "kyc_source": "$kycSource",
        "v4_errors": "$v4Errors",
        "bank_acc_num": "$bankDetails.accountNumber",
        "bank_ifsc": "$bankDetails.ifsc",
        "brand_name": "$brandName",
        "permanent_addr": "$permanentAddr",
        "requested_at": "$requestedAt",
        "accepted_at": "$acceptedAt",
        "created_at": "$updatedMIDInfo.createdAt",
        "updated_at": "$updatedAt"}}
    cols = ['obj_id', 'full_name', 'member_id', 'brand_name', 'contact_no', 'city', 'state', 'long', 'lat',
            'permanent_addr',
            'bank_acc_num', 'bank_ifsc', 'kycrequest_status', 'kyc_source', 'v4_errors', 'requested_at', 'accepted_at',
            'created_at', 'updated_at']
    query = {"$match": {audit_col: {"$gte": start_day, "$lt": end_day}}}
    pipeline = [query, proj]
    print(pipeline)
    mongo_data = _coll.aggregate(pipeline)
    arr = []
    for data in mongo_data:
        # print(data)
        arr.append(data)
    df = pd.DataFrame(arr, columns=cols)
    # df=pd.DataFrame(arr)
    return df


def extract_data(ds):
    print("Execution date is: {}".format(ds))
    day = ds
    # audit column name
    # day="2023-02-21"
    end_day = datetime.strptime(day, '%Y-%m-%d') + timedelta(1)
    start_day = end_day - timedelta(5)
    print("Running for date>= {} and date<{}".format(start_day, end_day))
    audit_col = metadata['audit_col']
    df = fetch_data(audit_col, start_day, end_day)
    print("Data Extracted. Row count is {}".format(df.shape[0]))
    if (df.shape[0] == 0):
        print("No data found, exiting!!")
        return
    # df=data_prep(df)
    print("Date cleaned and prepared, saving to the landing zone.")
    save_data(df)

#To be used for Full load
def extract_data2(st_dt, end_dt):
    start_day = datetime.strptime(st_dt, '%Y%m%d') + timedelta(1)
    end_day = datetime.strptime(end_dt, '%Y%m%d') + timedelta(1)
    print("Running for date>= {} and date<{}".format(start_day, end_day))
    audit_col = metadata['audit_col']
    df = fetch_data(audit_col, start_day, end_day)
    print("Data Extracted. Row count is {}".format(df.shape[0]))
    if (df.shape[0] == 0):
        print("No data found, exiting!!")
        return
    df=data_prep(df)
    print("Date cleaned and prepared, saving to the landing zone.")
    save_data(df)

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
    dag_id='mongo_athena_mobikwik_kyc_data',
    default_args=args,
    schedule_interval='25 8 * * *',
    start_date=datetime(2018, 1, 1, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=120),
    tags=['dataengg', 'mongo', 'athena', 'replication'],
    catchup=False
)

extract_mongo_data = PythonOperator(
    task_id='extract_mongo_data',
    provide_context=True,
    python_callable=extract_data,
    dag=dag
)

glue_load_refresh_data = BashOperator(
    task_id='glue_load_refresh_data',
    bash_command="sh /apps/nifi/glue_job_run.sh s3://mbk-nifi-landingzone/Glue/mg_mobikwik_kyc_data.txt mg_mobikwik kyc_data ",
    dag=dag
)

# sh /apps/nifi/glue_job_run.sh s3://mbk-nifi-landingzone/Glue/mg_mobikwik_onboarding_request.txt mg_mobikwik onboarding_request


extract_mongo_data >> glue_load_refresh_data

if __name__ == "__main__":
    dag.cli()
