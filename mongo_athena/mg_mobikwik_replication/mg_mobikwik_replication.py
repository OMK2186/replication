__author__ = 'ketankk'

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import pendulum
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import pendulum

from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import uuid

# mongo_athena_mobikwik_onboarding_request

from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import uuid

_mongo_host = '10.10.150.149'
_mongo_user = 'analytics'
_mongo_password = '3Fgs6&hs1cvbHjkk'
_mongo_port = 27017
_collection = 'subclientparams'
_mongo_db = 'mobikwik'

#.find({"":{$eq:"7869191546@nocash.mobikwik.com"}}).pretty();


coll_list = [
    {
    "collection": "kyc_data",
    "audit_col": "timestamp",
    "columns":["memberId", "contactNo", "kycRequestStatus"]
        #  "updatedAt" : ISODate("2023-04-18T10:02:04.488Z"),

    },
    {
        "collection": "subclientparams",
        "audit_col": "timestamp",
    }
]


metadata = {
    "schema": "mg_mobikwik",
    "table": "subclientparams",
    "audit_col": "timestamp",
    "bucket": "mbk-nifi-landingzone",
    "datapath": "/data/"}

'''
{
        "_id" : ObjectId("63b0e499e4b00ede217acc8c"),
        "mid" : "MBK0325670",
        "orderid" : "20230101071041185fdc7b4b3e",
        "subclienttype" : "302",
        "merchanttype" : "",
        "apiname" : "P2M",
        "txntype" : "DEBIT",
        "timestamp" : "2023-01-01 07:10:41",
        "ipAddress" : "2401:4900:7019:8527:dcf3:bb2a:dfae:c66f",
        "latitude" : "20.2747819",
        "longitude" : "85.8801215",
        "param1" : "",
        "param2" : "",
        "param3" : "",
        "param4" : "",
        "param5" : ""
}
'''


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
    df['created_at'].fillna('2001-01-01', inplace=True)
    df['created_at'] = df['created_at'].astype('datetime64[s]').astype('str')
    df['obj_id'] = df['obj_id'].astype('str')
    # df['latitude']=df['latitude'].astype('float')
    # df['longitude']=df['longitude'].astype('float')
    # df.drop('_id', axis=1, inplace=True)
    return df


# aws s3 cp /tmp/mg_mobikwik_subclientparams.txt s3://mbk-nifi-landingzone/Glue/mg_mobikwik_subclientparams.txt
# aws s3 cp s3://mbk-nifi-landingzone/Glue/mg_mobikwik_kyc_data.txt /tmp/mg_mobikwik_subclientparams.txt


def fetch_data(audit_col, start_day, end_day):
    _coll = get_mongo_connection()
    # Need these columns, projection used for creating aliases
    # TODO Changes required here;
    proj = {"$project": {
        "obj_id": "$_id",
        "mid": "$mid",
        "order_id": "$orderid",
        "subclient_type": "$subclienttype",
        "merchant_type": "$merchanttype",
        "api_name": "$apiname",
        "txn_type": "$txntype",
        "created_at": "$timestamp",
        "ip_address": "$ipAddress",
        "latitude": "$latitude",
        "longitude": "$longitude",
        "param1": "$param1",
        "param2": "$param2",
        "param3": "$param3",
        "param4": "$param4",
        "param5": "$param5"}}
    # Audit column-timestamp in this collection is string format, so parsing this date as str
    # cols=['obj_id', 'full_name', 'member_id', 'contact_no', 'city', 'state', 'kycrequest_status', 'v4_errors', 'created_at', 'updated_at']
    cols = ["obj_id", "mid", "order_id", "subclient_type", "merchant_type", "api_name", "txn_type", "created_at",
            "ip_address", "latitude", "longitude", "param1", "param2", "param3", "param4", "param5"]
    query = {"$match": {audit_col: {"$gte": str(start_day), "$lt": str(end_day)}}}
    print(query)
    pipeline = [query, proj]
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
    # day="2023-01-01"
    end_day = datetime.strptime(day, '%Y-%m-%d') + timedelta(1)
    start_day = end_day - timedelta(5)
    print("Running for date>= {} and date<{}".format(start_day, end_day))
    audit_col = metadata['audit_col']
    df = fetch_data(audit_col, start_day, end_day)
    print("Data Extracted. Row count is {}".format(df.shape[0]))
    if (df.shape[0] == 0):
        print("No data found, exiting!!")
        return
    df = data_prep(df)
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
    dag_id='mg_mobikwik_replication',
    default_args=args,
    schedule_interval='45 8 * * *',
    start_date=datetime(2018, 1, 1, tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=120),
    tags=['dataengg', 'mongo', 'athena', 'replication', 'mg_mobikwik', 'subclientparams'],
    catchup=False
)

extract_mongo_data = PythonOperator(
    task_id='extract_mongo_data-subclientparams',
    provide_context=True,
    python_callable=extract_data,
    dag=dag
)

glue_load_refresh_data = BashOperator(
    task_id='glue_load_refresh_data-subclientparams',
    bash_command="sh /apps/nifi/glue_job_run.sh s3://mbk-nifi-landingzone/Glue/mg_mobikwik_subclientparams.txt mg_mobikwik subclientparams ",
    dag=dag
)

# sh /apps/nifi/glue_job_run.sh s3://mbk-nifi-landingzone/Glue/mg_mobikwik_onboarding_request.txt mg_mobikwik onboarding_request


extract_mongo_data >> glue_load_refresh_data

if __name__ == "__main__":
    dag.cli()
