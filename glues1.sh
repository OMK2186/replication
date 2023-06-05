#!/bin/bash
GlueParam=$1
db=$2
table=$3
param1='--table_metafile'
#param2='$param1""=""$GlueParam'
#echo "$param2"
#echo {\"$param1\":\"$GlueParam\"}
job_id=$(aws glue start-job-run --job-name memberbalance-daily-rep --arguments {\"$param1\":\"$GlueParam\"} | jq '.JobRunId' | sed 's/"//g')
echo $job_id


for (( i=1; i<=9999999; i++))
do
status=$(aws glue get-job-run --job-name memberbalance-daily-rep --run-id $job_id | jq '.JobRun.JobRunState' | sed 's/"//g')
echo $status
if [[ $status = 'SUCCEEDED' ]]; then
echo "Glue Job Succeeded"
python3 /apps/nifi/partition_refresh/partition_refresh.py $2 $3
echo "Exit 0"
exit 0
fi

if [[ $status = 'FAILED' ]]; then
echo "Glue Job Failed"
echo "Exit 1"
exit 1
fi

if [[ $i -ge 500 ]]; then
echo "Reached maximum time allowed"
echo "Exit 2"
exit 2
fi

sleep 15

done

