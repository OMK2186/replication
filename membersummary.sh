rm /home/analyticusr/Laxmi/mongo_extract/output/AB1.csv


### input file name
x=`cat /home/analyticusr/Zohra/Trash/temp.csv`
c=0

pass="3Fgs6&hs1cvbHjkk"


for name in $x
do
echo "working for _id " $name
mongoexport -v --port 27017 -u analytics-app -p'Analytics@890#31090' --authenticationDatabase "giftcard" --host 10.10.150.147 --db giftcard -c transactions --query '{"_id":{"$oid":"$name"}' --type=csv --fields=_id,status,qcOrderId  --out /home/analyticusr/Laxmi/mongo_extract/output/insurance.csv 
cat /home/analyticusr/Laxmi/mongo_extract/output/insurance.csv | tail -n+2 >> /home/analyticusr/Laxmi/mongo_extract/output/insurance1.csv
((c++))
done

/home/analyticusr/Laxmi/mongo_extract/giftcard.sh (END)



prefix='bureau_variables/bureau_variables_glue_output/2022-11-29_bureau_files/14/'




mongoexport -v --port 27017 -u analytics -p'3Fgs6&hs1cvbHjkk' --authenticationDatabase "mobikwik" --host 10.10.150.149 --db mobikwik -c memberSummary --query '{"_id":{"$oid":"$name"}' --type=csv --fields=_id, location.transactionId,location.transactionType,location.location.coordinates,location.updatedAt  --out /tmp/latlng.csv 



query='{{"_id":"himanshu.style009@gmail.com"},{"location.transactionId":1,"location.transactionType":1,"location.location.coordinates":1,"location.updatedAt":1}}'
echo $query

query='{"_id":"himanshu.style009@gmail.com"}'
echo $query


mongoexport -v --port 27017 -u analytics -p'3Fgs6&hs1cvbHjkk' --authenticationDatabase "mobikwik" --host 10.10.150.149 --db mobikwik -c memberSummary --query $query --type=csv --fields=_id --out /tmp/latlng.csv 

mongoexport -v --port 27017 -u analytics -p'3Fgs6&hs1cvbHjkk' --authenticationDatabase "mobikwik" --host 10.10.150.149 --db mobikwik -c memberSummary --query $query --type=csv --fields=_id,location --out /tmp/latlng.csv 



mongors:SECONDARY> db.memberSummary.find({"_id":"himanshu.style009@gmail.com","location.updatedAt":{$gte:ISODate('2022-11-01')}},{"location.transactionId":1,"location.transactionType":1,"location.location.coordinates":1,"location.updatedAt":1}).pretty();




{"_id":"himanshu.style009@gmail.com"},{"location.transactionId":1,"location.transactionType":1,"location.location.coordinates":1,"location.updatedAt":1}


#--[data-eng-new@data-analytics-common-nifi-prod-01 centos]$ mongo --port 27017 -u 'analytics' -p'3Fgs6&hs1cvbHjkk'  --authenticationDatabase 'mobikwik'  --host 10.10.150.149

db.memberSummary.find

location.transactionId":1,"location.transactionType":1,"location.location.coordinates

=========================


query="{'location.updatedAt':{$gte:ISODate('2022-12-01')}}"

echo $query



query='{"location.updatedAt":{$gte:{"$date":"2022-12-01T00:00:00.000Z"}}}'
echo $query

mongoexport -v --port 27017 -u analytics -p'3Fgs6&hs1cvbHjkk' --authenticationDatabase "mobikwik" --host 10.10.150.149 --db mobikwik -c memberSummary --query $query --type=csv --fields=_id,location.transactionId,location.transactionType,location.updatedAt --out /tmp/latlng.csv 

==============================
> query='{"location.updatedAt":{"$gte":{"$date":"2022-12-01T00:00:00.000Z"}}}'

> mongoexport -v --port 27017 -u analytics -p'3Fgs6&hs1cvbHjkk' --authenticationDatabase "mobikwik" --host 10.10.150.149 --db mobikwik -c memberSummary --query $query --type=csv --fields=_id,location.transactionId,location.transactionType,location.location.coordinates,location.updatedAt --out /tmp/latlng.csv 



====

mongoexport --db ium --collection events \
  --query '{ 
    "created_at": { 
      "$gte": { "$date": "2016-03-01T00:00:00.001Z" },
      "$lte": { "$date": "2016-03-29T23:59:59.000Z" }
    },
    "name": "UPDATE_SUCCESS"
  }' \