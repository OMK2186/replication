Mongo replication---

date '+%Y-%m-%d 00:00:00' -d "3 days ago"


 db.surveyresponsedetails.find({"updatedAt":{$gte:("2022-11-27 00:00:00")}})


 db.surveyresponsedetails.find({"updatedAt":{$gte:("2022-11-27 00:00:00")}},{'mbkLoanId':true,'surveyFilled':true,'surveyOptionsSelected':true, 'surveyShownCount':true, 'updatedAt':true, 'createdAt':true, _id':false })

{ "_id" : ObjectId("6382ace746e0fb001de07229"), "mbkLoanId" : "7001423571020563636", "surveyFilled" : false, "surveyShownCount" : 3, "createdAt" : "2022-11-27T05:48:47.951+05:30", "updatedAt" : "2022-11-29T08:37:51.708+05:30" }

db.surveyresponsedetails.find({"updatedAt":{$gte:("2022-11-27 00:00:00")}},{'mbkLoanId':true,'surveyFilled':true,'surveyOptionsSelected':{$slice: 1}, 'surveyShownCount':true, 'updatedAt':true, 'createdAt':true, '_id':false })



===============

mongoexport --host 10.10.150.189  -u "DataEnggUser" -p "MwGFWoyUeJxK" --authenticationDatabase "lending" --db lending --collection surveyresponsedetails --json --out text.csv -q {"updatedAt":{$gte:("2022-11-27 00:00:00")}}


mongoexport --host 10.10.150.189  -u "DataEnggUser" -p "MwGFWoyUeJxK" --authenticationDatabase "lending" --db lending --collection surveyresponsedetails --json --out text.csv -q "{'updatedAt':{$gte:('2022-11-27 00:00:00')}}"
mongodump --host 10.10.150.189  -u "DataEnggUser" -p "MwGFWoyUeJxK" --authenticationDatabase "lending" --db lending --collection surveyresponsedetails --csv --out text.csv -q "{'updatedAt':{$gte:('2022-11-27 00:00:00')}}"

mongodump --host 10.10.150.226  -u "analytics" -p "3Fgs6\&hs1cvbHjkk" --authenticationDatabase "mobikwik" --db mobikwik --collection surveyresponsedetails --csv --out text.csv -q "{'updatedAt':{$gte:('2022-11-27 00:00:00')}}"


mongoexport --uri=mongodb://DataEnggUser:MwGFWoyUeJxK@10.10.150.189:27017/lending --collection surveyresponsedetails --authenticationDatabase lending --out output.json


mongoexport --port 27017 -u 'analytics' -p'3Fgs6&hs1cvbHjkk'  --authenticationDatabase 'mobikwik'  --host 10.10.150.149 --db mobikwik --collection kyc_data --csv --out /tmp/kk_tmp1.csv -q "{'_id':{$eq:'1t5skin46n89iqu'}"

db.kyc_data.find({'_id':{$eq:'1t5skin46n89iqu'}});


import pymongo
conn = pymongo.MongoClient('mongodb://"DataEnggUser":"MwGFWoyUeJxK"@10.10.150.189:27017/')


db = conn['lending']
coll = db['surveyresponsedetails']


from pymongo import MongoClient
client = MongoClient("mongodb://analytics:3Fgs6&hs1cvbHjkk@10.10.150.149:27017")
db=client.mobikwik
db.command("serverStatus")

db = client.{database}
try: db.command("serverStatus")
except Exception as e: print(e)
else: print("You are connected!")
client.close()


mongoexport --port 27017 -u 'analytics' -p'3Fgs6&hs1cvbHjkk'  --authenticationDatabase 'mobikwik'  --host 10.10.150.149


mongoexport --host 10.10.150.189  -u "DataEnggUser" -p "MwGFWoyUeJxK" --authenticationDatabase "lending" --db lending --collection surveyresponsedetails --csv --out text.csv --fields mbkLoanId,surveyFilled,surveyOptionsSelected, surveyShownCount, updatedAt, createdAt -q '{"updatedAt":{"$gt":new Date(ISODate().getTime() - 1000 * 86400 * 3)}}' 

lendingmongo:SECONDARY> db.surveyresponsedetails.find({},{updatedAt:{$gt:ISODate('2022-11-01'), $lt: ISODate('2022-11-21')}})



{ "_id" : ObjectId("6380512146e0fb002e9c8f9c"), "" : "7001776563060666976", "" : true, "surveyOptionsSelected" : [ "I don't require the loan amount now" ], "surveyShownCount" : 1, "createdAt" : "2022-11-25T10:52:41.212+05:30", "updatedAt" : "2022-11-25T10:52:41.212+05:30" }


db.surveyresponsedetails.find({"updatedAt" :{ $gte:ISODate('2022-11-01')}})
 db.surveyresponsedetails.find({"updatedAt" :{ $gte:"2022-11-28T18:41:23.298+05:30"}})

lendingmongo:SECONDARY> db.surveyresponsedetails.find({"updatedAt" :{ $gte:"2022-11-28T18:41:23.298+05:30"}})

mongoexport -v --host "10.10.150.189" -u "DataEnggUser" -p "MwGFWoyUeJxK" --authenticationDatabase lending --db lending -c surveyresponsedetails --port 27017 --query '{"updatedAt":{$gte:("2022-11-28")}}' --type=csv --out 

--fields mbkLoanId,surveyFilled,surveyOptionsSelected, surveyShownCount, updatedAt, createdAt


myclient = pymongo.MongoClient("10.10.150.189")
myclient.test.authenticate("DataEnggUser","MwGFWoyUeJxK")



-----------------------------

CREATE TABLE mobikwik_Schema.mongo_lending_surveyresponsedetails(
  mbkloanid varchar(100), 
  surveyfilled boolean, 
  surveyshowncount int, 
  createdat timestamp, 
  updatedat timestamp, 
  surveyoptionsselected varchar(500),
  primary key(mbkloanid));


  mongoexport -v --host "" -u "" -p "" --authenticationDatabase lending --db lending -c surveyresponsedetails --port 27017 --query '{"updatedAt":{$gte:("2022-11-28")}}' --type=csv --out 
