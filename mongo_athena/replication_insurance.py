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
            #client=MongoClient(_mongo_host,_mongo_port,username=_mongo_user,password=_mongo_password,authSource=_mongo_db)
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

def main():
    df=get_mongo_data()
    df['_id']=df['_id'].astype(str)
    final_df = pd.DataFrame(columns=['_id','mobile','accountID','memberId','consentID','linkedReferenceNumber','transactions_startDate','transactions_endDate','type','mode','amount','currentBalance','transactionTimestamp','valueDate','txnId','narration','reference','dataLife','reportedDate','createdAt','updatedAt'])
    final_df2 = pd.DataFrame(columns=['_id','mobile','accountID','memberId','consentID','linkedReferenceNumber','transactions_startDate','transactions_endDate','type','mode','amount','currentBalance','transactionTimestamp','valueDate','txnId','narration','reference','dataLife','reportedDate','createdAt','updatedAt'])
    print(len(df['_id']))
    for i in range(len(df['_id'])):
        print(i)
        id=df['_id'][i]
        print(id) 
        print('a')
        df2=df['transactions'].loc[df['_id'] == id]
        print('a')
        df3=pd.json_normalize(df2)
        print('a')
        if 'startDate' not in list(df3.columns):
            df3['startDate']=None
        if 'endDate' not in list(df3.columns):
            df3['endDate']=None
        df3=df3.rename(columns={'startDate':'transactions_startDate'})
        print('a')
        df3=df3.rename(columns={'endDate':'transactions_endDate'})
        print('a')
        print(df3)
        print(len(df3['transaction'][0]))
        if len(df3['transaction'][0])>= 1 :
            print('1st_loop')
            df4=df3['transaction'].explode('transaction').reset_index()
            print(df4)
            df5=pd.json_normalize(df4['transaction'])
            print(df5)
            startDate=df3['transactions_startDate'][0]
            print('a')
            endDate=df3['transactions_endDate'][0]
            print('a')
            df5['_id']=id
            print('a')
            df5['transactions_startDate']=startDate
            print('a')
            df5['transactions_endDate']=endDate
            print('a')
            internal_df=df[['_id','mobile','accountID','memberId','consentID','linkedReferenceNumber','createdAt','updatedAt']].loc[df['_id'] == id]
            print('a')
            df_global=pd.merge(df5,internal_df, how='left',on='_id')
            print('a')
            #print(df_global)
            final_df=pd.concat([final_df,df_global])
            print(final_df)
        if len(df3['transaction'][0]) == 0: 
           
            print(id)
            df5['_id']=id
            print(df5)
            internal_df=df[['_id','mobile','accountID','memberId','consentID','linkedReferenceNumber','createdAt','updatedAt']].loc[df['_id'] == id]
            internal_df[['transactions_startDate','transactions_endDate','type','mode','amount','currentBalance','transactionTimestamp','valueDate','txnId','narration','reference','dataLife','reportedDate']]=None
            print(internal_df)
            final_df2=pd.concat([final_df2,internal_df])
            print(final_df2)
    		
    final_df2['createdAt']=final_df2['createdAt'].astype('datetime64[ns]') + timedelta(hours=5.5)
    final_df2['updatedAt']=final_df2['updatedAt'].astype('datetime64[ns]') + timedelta(hours=5.5)		
    
    final_df['createdAt']=final_df['createdAt'].astype('datetime64[ns]') + timedelta(hours=5.5)
    final_df['updatedAt']=final_df['updatedAt'].astype('datetime64[ns]') + timedelta(hours=5.5)
    final_df['reportedDate']=final_df['reportedDate'].astype('datetime64[ns]') + timedelta(hours=5.5)
    
    
    final_df2.to_parquet('s3://mbk-nifi-landingzone/data/lending_schema/rule_matadata/op_final_df2.parquet')
    final_df.to_parquet('s3://mbk-nifi-landingzone/data/lending_schema/rule_matadata/op_final_df.parquet')

main()


