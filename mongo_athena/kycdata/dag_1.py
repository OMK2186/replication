from datetime import timedelta,datetime
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

_mongo_host='10.10.150.149'
_mongo_user='analytics'
_mongo_password='3Fgs6&hs1cvbHjkk'
_mongo_port=27017
_collection='onboarding_request'
_mongo_db='mobikwik'

metadata={
"schema":"mg_mobikwik",
"table":"onboarding_request",
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

    df.fillna('', inplace=True)

    #Removing Millisecs from datetime
    df['updated_at'] = df['updated_at'].astype('datetime64[s]').astype('str')
    df['created_at'] = df['created_at'].astype('datetime64[s]').astype('str')
    df['obj_id']=df['obj_id'].astype('str')
    df['retailagent_id']=df['retailagent_id'].astype('str')
    df['self_onboarding']=df['self_onboarding'].astype('str')

    df.drop('_id', axis=1, inplace=True)
    return df


def fetch_data(audit_col, start_day, end_day):

    _coll=get_mongo_connection()

    # Need these columns, projection used for creating aliases
    proj={"$project":{
    "obj_id":"$_id",
    "member_id":"$memberId",
    "legal_name":"$legalName",
    "brand_name":"$brandName",
    "qr_code":"$qrCode",
    "self_onboarding":"$isSelfOnboarding",
    "permanent_address":"$permanentAddress",
    "city":"$city",
    "user_kyc":"$userKyc",
    "category":"$category",
    "retailagent_id":"$retailAgent._id",
    "retailagent_memberid":"$retailAgent.memberId",
    "ur_lat":"$userRequest.latitude",
    "ur_long":"$userRequest.longitude", 
    "created_at":"$createdAt",
    "updated_at":"$updatedAt"}}

    query={"$match":{audit_col:{"$gte":start_day, "$lt":end_day}}}
    pipeline=[query, proj]
    mongo_data=_coll.aggregate(pipeline)
    arr=[]
    for data in mongo_data:
        arr.append(data)

    df=pd.DataFrame(arr)
    return df




def extract_data(ds):
    print("Execution date is: {}".format(ds))
    day=ds

    #audit column name
    #day="2023-02-21"
    end_day=datetime.strptime(day,'%Y-%m-%d')
    start_day=end_day-timedelta(5)
    print("Running for date>= {} and date<{}".format(start_day, end_day))

    audit_col = metadata['audit_col']
    df=fetch_data(audit_col, start_day, end_day)
    print("Data Extracted. Row count is {}".format(df.shape[0]))
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
    dag_id='mongo_athena_mobikwik_onboarding_request',
    default_args=args,
    schedule_interval='20 11 * * *',
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


glue_load_refresh_data = BashOperator(
        task_id='glue_load_refresh_data',
        bash_command="sh /apps/nifi/glue_job_run.sh s3://mbk-nifi-landingzone/Glue/mg_mobikwik_onboarding_request.txt mg_mobikwik onboarding_request ",
        dag=dag
    )

#sh /apps/nifi/glue_job_run.sh s3://mbk-nifi-landingzone/Glue/mg_mobikwik_onboarding_request.txt mg_mobikwik onboarding_request



extract_mongo_data>>glue_load_refresh_data

if __name__ == "__main__":
    dag.cli()