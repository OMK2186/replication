epfo_df =
epfo_list = epfo_df.to_dict(orient='records')
5:24
consolidated_df = pd.DataFrame()
for i in epfo_list:
    df = pd.json_normalize(i)
    df=df.rename(columns={'memberId':'member_id','totalBalance':'overall_total_balance'})
    df1=df.explode(column='epfoUanList')
    df2=df1.join(pd.json_normalize(df1.pop('epfoUanList')))
    df3=df2.explode(column='epfoEmployers').reset_index(drop=True)
    df4=df3.join(pd.json_normalize(df3.pop('epfoEmployers')))
    df5=df4.explode(column='epfoUserEmploymentData.epfoPassbookEntryEntries').reset_index(drop=True)
    df6=df5.join(pd.json_normalize(df5.pop('epfoUserEmploymentData.epfoPassbookEntryEntries')))
    cleaned_df = df6[['memberId','lastUpdatedAt','mobileNumber','overall_total_balance','establishmentName',
                      'epfoUserEmploymentData.dateOfBirth','epfoUserEmploymentData.dateOfJoining',
                      'epfoUserEmploymentData.totalEmployeeShare','epfoUserEmploymentData.totalEmployerShare',
                      'epfoUserEmploymentData.totalPensionShare','epfoUserEmploymentData.name',
                      'epfoUserEmploymentData.fatherName','employeeShare','employerShare','pensionShare',
                      'totalBalance','dateOfApproval']]
    consolidated_df = pd.concat([consolidated_df,cleaned_df])




    =======

    df = pd.json_normalize(data2)
df = df.rename(columns={'mobile':'mobile_number'})
df1 = df.explode(column='accounts').reset_index(drop=True)
df2 = df1.join(pd.json_normalize(df1.pop('accounts')))
try:
    df3 = df2.explode(column='profile').reset_index(drop=True)
    df4 = df3.join(pd.json_normalize(df3.pop('profile')))
except:
    df4["name"] = ""
    df4["dob"] = ""
    df4["mobile"] = ""
    df4["nominee"] = ""
    df4["landline"] = ""
    df4["address"] = ""
    df4["email"] = ""
    df4["pan"] = ""
    df4["ckycCompliance"] = ""
consolidated_df = pd.concat([consolidated_df,df4])


=====

acc_statement =
acc_statement_list = acc_statement.to_dict(orient='records')


consolidated_df = pd.DataFrame()
for i in acc_statement_list:
    df = pd.json_normalize(i)
    df = df.rename(columns={'mobile':'mobile_number'})
    df1 = df.explode(column='accounts').reset_index(drop=True)
    df2 = df1.join(pd.json_normalize(df1.pop('accounts')))
    df3 = df2.explode(column='profile').reset_index(drop=True)
    df4 = df3.join(pd.json_normalize(df3.pop('profile')))
    consolidated_df = pd.concat([consolidated_df,df4])




==========

#mongo --port 27017 -u analytics-app -p'Analytics@890#31090' --authenticationDatabase 'insurance' --host 10.10.150.147

import pandas as pd
import numpy as np
from datetime import timedelta,datetime
import sys
from dateutil import tz
import dateutil
from pymongo import MongoClient
import uuid
_mongo_host='10.10.150.147'
_mongo_user='analytics-app'
_mongo_password='Analytics@890#31090'
_mongo_port=27017
_collection='accountTransactions'
_mongo_db='insurance'
audit_col='createdAt'


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

def get_mongo_data():
    _coll=get_mongo_connection()
    mongo_data=_coll.find()
    df=pd.DataFrame(mongo_data)
    print("data fetched")
    return df

def extract_data(ds):
    print("Execution date is: {}".format(ds))
    day=ds

    #audit column name
    #day="2023-02-21"
    end_day=datetime.strptime(day,'%Y-%m-%d') + timedelta(1)
    start_day=end_day-timedelta(5)
    print("Running for date>= {} and date<{}".format(start_day, end_day))

    audit_col = metadata['audit_col']
    df=fetch_data(audit_col, start_day, end_day)
    print("Data Extracted. Row count is {}".format(df.shape[0]))
    df=data_prep(df)
    print("Date cleaned and prepared, saving to the landing zone.")
    save_data(df)



 def extract():
 



def transform()

 

def load():


def epfo_data():





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
    dag_id='mongo_athena_mg_insurance',
    default_args=args,
    schedule_interval='20 9 * * *',
    start_date=datetime(2018,1,1,tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=120),
    tags=['dataengg', 'mongo', 'athena', 'replication', 'insurance', 'epfoUserEmploymentData', 'accountstatement', 'accountTransactions'],
    catchup=False
    )




start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)


table_list=['epfoUserEmploymentData'.'accountstatement','accountTransactions']

for table in table_list:

    extract_mongo_data = PythonOperator(task_id=f'mongo-etl-{table}', provide_context=True, python_callable=extract_data, dag=dag)
    start >> extract_mongo_data
    glue_job = PythonOperator(task_id=f'glue-{table}', provide_context=True, python_callable=run_glue, dag=dag)
    extract_mongo_data >> glue_job
    glue_job >> end



if __name__ == "__main__":
    dag.cli()





######


#epfo_df =
#epfo_list = epfo_df.to_dict(orient='records')

consolidated_df = pd.DataFrame()
for i in epfo_list:
    df = pd.json_normalize(i)
    df=df.rename(columns={'memberId':'member_id','totalBalance':'overall_total_balance'})
    df1=df.explode(column='epfoUanList')
    df2=df1.join(pd.json_normalize(df1.pop('epfoUanList')))
    df3=df2.explode(column='epfoEmployers').reset_index(drop=True)
    df4=df3.join(pd.json_normalize(df3.pop('epfoEmployers')))
    df5=df4.explode(column='epfoUserEmploymentData.epfoPassbookEntryEntries').reset_index(drop=True)
    df6=df5.join(pd.json_normalize(df5.pop('epfoUserEmploymentData.epfoPassbookEntryEntries')))
    cleaned_df = df6[['memberId','lastUpdatedAt','mobileNumber','overall_total_balance','establishmentName',
                      'epfoUserEmploymentData.dateOfBirth','epfoUserEmploymentData.dateOfJoining',
                      'epfoUserEmploymentData.totalEmployeeShare','epfoUserEmploymentData.totalEmployerShare',
                      'epfoUserEmploymentData.totalPensionShare','epfoUserEmploymentData.name',
                      'epfoUserEmploymentData.fatherName','employeeShare','employerShare','pensionShare',
                      'totalBalance','dateOfApproval']]
    consolidated_df = pd.concat([consolidated_df,cleaned_df])





    df = pd.json_normalize(data2)
df = df.rename(columns={'mobile':'mobile_number'})
df1 = df.explode(column='accounts').reset_index(drop=True)
df2 = df1.join(pd.json_normalize(df1.pop('accounts')))
try:
    df3 = df2.explode(column='profile').reset_index(drop=True)
    df4 = df3.join(pd.json_normalize(df3.pop('profile')))
except:
    df4["name"] = ""
    df4["dob"] = ""
    df4["mobile"] = ""
    df4["nominee"] = ""
    df4["landline"] = ""
    df4["address"] = ""
    df4["email"] = ""
    df4["pan"] = ""
    df4["ckycCompliance"] = ""
consolidated_df = pd.concat([consolidated_df,df4])



acc_statement =
acc_statement_list = acc_statement.to_dict(orient='records')


consolidated_df = pd.DataFrame()
for i in acc_statement_list:
    df = pd.json_normalize(i)
    df = df.rename(columns={'mobile':'mobile_number'})
    df1 = df.explode(column='accounts').reset_index(drop=True)
    df2 = df1.join(pd.json_normalize(df1.pop('accounts')))
    df3 = df2.explode(column='profile').reset_index(drop=True)
    df4 = df3.join(pd.json_normalize(df3.pop('profile')))
    consolidated_df = pd.concat([consolidated_df,df4])






