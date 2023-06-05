
table_dict={"schema":"","table":"","src_loc":"","target_loc":"","partition_col":"","":""}
target_path=""
source_path=""

def sync_cdc():
    try:

        new_day = updated_data.select('day').distinct().collect()
        output = [i[0] for i in new_day]
        output = str(tuple(output)).rstrip(',)') + ')'
        pushdownpredicate = "(day in " + str(output) + ")"
        logger.info('Read Existing Data - New Day partition '+pushdownpredicate)
        
        existing_data = glueContext.create_dynamic_frame.from_catalog(database=table_dict['schema'],
                                                                   table_name=table_dict['table'],
                                                                   push_down_predicate=pushdownpredicate).toDF()

        logger.info('Read Existing Data Complete')
        if not final_data.rdd.isEmpty():
            logger.info('Data Merge Started')
            old_data = final_data.join(delta_day_double, pk_list, "left_anti")
            merged_data = customUnion(old_data,delta_day_double)
            logger.info('Data Merge Completed')
        else:
            merged_data = delta_day_double
        logger.info('Write Final Data Started')
        merged_data.repartition(table_dict['PartitionCol']).write.mode('overwrite').format('parquet').partitionBy(
            table_dict['partition_col']).save(target_path)

        logger.info('Write Final Data completed')
        glueContext.purge_s3_path(source_path,{"retentionPeriod":0})
    except Exception as e:
        raise e
        logger.info(e)
        print('sync_cdc failed')


####

import boto3
from botocore.exceptions import ClientError

def run_glue_job(job_name, arguments = {}):
   session = boto3.session.Session()
   glue_client = session.client('glue')
   try:
      job_run_id = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
   return job_run_id
   except ClientError as e:
      raise Exception( "boto3 client error in run_glue_job: " + e.__str__())
   except Exception as e:
      raise Exception( "Unexpected error in run_glue_job: " + e.__str__())

print(run_glue_job("run_s3_file_job"))

aws glue start-job-run --job-name mbk_transform_updated --arguments {\"$param1\":\"$GlueParam\"} | jq '.JobRunId' 

table_dict={"schema":"","table":"","src_loc":"","target_loc":"","partition_col":"","":""}


for (( i=1; i<=9999999; i++))
do
status=$(aws glue get-job-run --job-name mbk_transform_updated --run-id $job_id | jq '.JobRun.JobRunState' | sed 's/"//g')
echo $status
if [[ $status = 'SUCCEEDED' ]]; then
echo "Glue Job Succeeded"
python3 /apps/nifi/partition_refresh/partition_refresh.py $2 $3
echo "Exit 0"
exit 0
fi


def run_glue_job_get_status(job_name, arguments = {}):
   session = boto3.session.Session()
   glue_client = session.client('glue')
   try:
      job_run_id = glue_client.start_job_run(JobName=job_name, Arguments=arguments)
      status_detail = glue_client.get_job_run(JobName=job_name, RunId = job_run_id.get("JobRunId"))
      status = status_detail.get("JobRun").get("JobRunState")
      return status
   except ClientError as e:
      raise Exception( "boto3 client error in run_glue_job_get_status: " + e.__str__())
   except Exception as e:
      raise Exception( "Unexpected error in run_glue_job_get_status: " + e.__str__())


job = glue.create_job(Name='sample', Role='Glue_DefaultRole',
                      Command={'Name': 'glueetl',
                               'ScriptLocation': 's3://my_script_bucket/scripts/my_etl_script.py'})



import boto3
glue = boto3.client(service_name='glue', region_name='us-east-1',
              endpoint_url='https://glue.us-east-1.amazonaws.com')

myJob = glue.create_job(Name='sample', Role='Glue_DefaultRole',
                          Command={'Name': 'glueetl',
                                   'ScriptLocation': 's3://my_script_bucket/scripts/my_etl_script.py'})

myNewJobRun = glue.start_job_run(JobName=myJob['Name'])

## Run a Glue job for





import awswrangler as wr
import pandas as pd
from datetime import datetime
# AWS-SDK-Pandas. With this library to can get the results in a blocking manner:

# Retrieving the data from Amazon Athena
df = wr.athena.read_sql_query("SELECT * FROM email_db.token_data_1 limit 10", database="email_db")