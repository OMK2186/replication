from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import pendulum

#mongo_athena_mobikwik_kyc_data.py

from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import uuid

_mongo_host='10.10.150.149'
_mongo_user='analytics'
_mongo_password='3Fgs6&hs1cvbHjkk'
_mongo_port=27017
_collection='kyc_data'
_mongo_db='mobikwik'

metadata={
"schema":"mg_mobikwik",
"table":"kyc_data",
"audit_col": "updatedAt", 
"bucket":"mbk-nifi-landingzone",
"datapath":"/data/"}

def get_mongo_connection():
    try:
        if _mongo_host and _mongo_db and _collection and _mongo_port and _mongo_user and _mongo_password:
            client=MongoClient(_mongo_host,_mongo_port,username=_mongo_user,password=_mongo_password,authSource=_mongo_db)
            db = client[_mongo_db]
            _coll=db[_collection]
            client.server_info()
    except Exception as e:
        print(e)
    return _coll



def save_data(df):
    data_path="s3://"+metadata['bucket']+metadata['datapath']+"mg_"+_mongo_db+"/"+_collection+"/"
    uq_id=str(uuid.uuid4())
    data_path=data_path+uq_id+".parquet"
    print("Saving File to location: {}".format(data_path))
    df.to_parquet(data_path, index=False)


def data_prep(df):
    df['updated_at'].fillna('2001-01-01', inplace=True)
    df['created_at'].fillna('2001-01-01', inplace=True)
        #TODO Changes required here;
    #Removing Millisecs from datetime
    df['updated_at'] = df['updated_at'].astype('datetime64[s]').astype('str')
    df['created_at'] = df['created_at'].astype('datetime64[s]').astype('str')
    df['obj_id']=df['obj_id'].astype('str')
    #df.drop('_id', axis=1, inplace=True)
    return df


def fetch_data(audit_col, start_day, end_day):
    _coll=get_mongo_connection()
    # Need these columns, projection used for creating aliases
    #TODO Changes required here;
    proj={"$project":{
    "obj_id":"$_id",
    "full_name":"$fullName",
    "member_id":"$memberId",
    "contact_no":"$contactNo",        
    "city":"$city",
    "state":"$state",
    "kycrequest_status":"$kycRequestStatus",
    "v4_errors":"$v4Errors",
    "created_at":"$updatedMIDInfo.createdAt",
    "updated_at":"$updatedAt"}}
    cols=['obj_id', 'full_name', 'member_id', 'contact_no', 'city', 'state', 'kycrequest_status', 'v4_errors', 'created_at', 'updated_at']
    query={"$match":{audit_col:{"$gte":start_day, "$lt":end_day}}}
    pipeline=[query, proj]
    mongo_data=_coll.aggregate(pipeline)
    arr=[]
    for data in mongo_data:
        #print(data)
        arr.append(data)
    df=pd.DataFrame(arr, columns=cols)
    #df=pd.DataFrame(arr)
    return df




def extract_data(ds):
    print("Execution date is: {}".format(ds))
    day=ds
    #audit column name
    #day="2023-02-21"
    end_day=datetime.strptime(day,'%Y-%m-%d')
    start_day=end_day-timedelta(5000)
    print("Running for date>= {} and date<{}".format(start_day, end_day))
    audit_col = metadata['audit_col']
    df=fetch_data(audit_col, start_day, end_day)
    print("Data Extracted. Row count is {}".format(df.shape[0]))
    if(df.shape[0]==0):
        print("No data found, exiting!!")
        return
    #df=data_prep(df)
    print("Date cleaned and prepared, saving to the landing zone.")
    save_data(df)




def ext(ds, **kwargs):
    print(ds)
    
    

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
    schedule_interval='55 11 * * *',
    start_date=datetime(2018,1,1,tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=120),
    tags=['dataengg','mongo','athena','replication'],
    catchup=False
    )


 

extract_mongo_data = PythonOperator(
    task_id='extract_mongo_data',
    provide_context=True,
    python_callable=extract_data,
    dag=dag
)

extract_mongo_data

if __name__ == "__main__":
    dag.cli()
