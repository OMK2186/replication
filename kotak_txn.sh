host='10.10.150.149'
db='mobikwik'
collection='kotakTransactions'
port=27017
mongouser='analytics'
mongopassword='3Fgs6&hs1cvbHjkk'
tgttable='mobikwikschema.kotaktransactions'


from pymongo import MongoClient
_mongo_host='10.10.150.149'
_mongo_user='analytics'
_mongo_password='3Fgs6&hs1cvbHjkk'
_mongo_port=27017
_collection='kotakTransactions'
_mongo_db='mobikwik'
#audit_col='updatedAt'

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




#mongoexport -v --host $host -u $mongouser -p $mongopassword --authenticationDatabase mobikwik --db %s -c %s --port %s --query '{"updateDate":{$gte:ISODate("2022-12-01")}}' --type=json --out

mongo -v --host $host -u $mongouser -p $mongopassword --authenticationDatabase mobikwik --db %s -c %s --port %s --query '{"updateDate":{$gte:ISODate("2022-12-01")}}' --type=json --out


op_list=['_id','userId','date','updateDate','source','mode','description','name','amount','amountIncludingFee','status','statusMessage','paymentFlow','cardHashId','clientId','subClientId','delayTimeStamp','UTR','provider','debitAccountType']

df=retrive_mongo_data(tgt_maxupdatedat)
print("retrived")
if df is None:
    sys.exit("df is empty | terminating the program")

print("mongo data converted to df")
df=df[op_list]
df['_id']=df['_id'].astype(str)
print("converting column createdAt to datetime")
df['date']=pd.json_normalize(df['date'])
df['date']=(pd.to_datetime(df['date'])).dt.tz_convert('Asia/Kolkata')
df['date']=df['date'].dt.tz_localize(None)

df['updateDate']=pd.json_normalize(df['updateDate'])
df['updateDate']=(pd.to_datetime(df['updateDate'])).dt.tz_convert('Asia/Kolkata')
df['updateDate']=df['updateDate'].dt.tz_localize(None)

df2=df[~pd.isna(df.delayTimeStamp)][['_id','delayTimeStamp']]
df2=df2.reset_index()
df2['delayTimeStamp']=pd.json_normalize(df2['delayTimeStamp'])['$date']
df2['delayTimeStamp']=(pd.to_datetime(df2['delayTimeStamp'])).dt.tz_convert('Asia/Kolkata')
df2['delayTimeStamp']=df2['delayTimeStamp'].dt.tz_localize(None)
del df2['index']

new=pd.merge(df,df2,how='left',on='_id')
del new['delayTimeStamp_x']
new.rename(columns={'delayTimeStamp_y':'delayTimeStamp'},inplace=True)

print("conversion to parquet ")
new.to_parquet(_localfs+'/output.parquet')
os.system(f"aws s3 cp {_localfs}/output.parquet s3://mbk-nifi-landingzone/data/mobikwik_schema/kotaktransactions/")

print("completed")


mongors:SECONDARY> db.kotakTransactions.find({'updateDate':{$gte:ISODate('2022-12-03'), $lt:ISODate('2022-12-04')}}).count()




