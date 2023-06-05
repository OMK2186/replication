import boto3
import time
from airflow.operators.python_operator import PythonOperator
import logging




logger = logging.getLogger(__name__)












def create_load_athena():
  #TODO Delete existing files
  delete_old_data()
  query1="CREATE EXTERNAL TABLE if not exists lending_schema.lending_xtra_vars(memberid string , investment_amount bigint , investment_count bigint , withdrawal_amount double , withdrawal_count bigint , total_attempted_investment_transaction bigint , success_txn_count bigint , success_txn_amount bigint , failed_txn_count bigint , failed_txn_amount bigint , first_successful_date timestamp , last_successful_date timestamp , first_attempted_date timestamp , last_attempted_date timestamp , first_investment_date timestamp , last_investment_date timestamp , last_withdrawal_date timestamp , current_investment double , current_investment_bucket varchar(6) , day_difference bigint , day_diff_bucket varchar(4) )ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' location 's3://mbk-datalake-common-prod/lending_schema/lending_xtra_vars/'"
  print(query1)
  #logging.inf("Query1: {}".format(query1))
  query2="insert into lending_schema.lending_xtra_vars  select a.*,b.current_investment as Current_Investment,case when cast(current_investment as decimal(18,2))<=1000 then '0-1000' when cast(current_investment as decimal(18,2))>1000 and cast(current_investment as decimal(18,2)) is not null then '>1000' end as Current_Investment_Bucket ,date_diff('day',date(First_investment_date),date(Last_Withdrawal_Date)) as Day_Difference, case when date_diff('day',date(First_investment_date),date(Last_Withdrawal_Date))<=15 then '0-15' when (date_diff('day',date(First_investment_date),date(Last_Withdrawal_Date))>15 and date_diff('day',date(First_investment_date),date(Last_Withdrawal_Date)) is not null) then '>15' end as Day_Diff_Bucket  from (  select MemberId,Investment_Amount,Investment_Count,Withdrawal_Amount,Withdrawal_Count,Total_Attempted_Investment_Transaction,Success_Txn_Count,Success_Txn_Amount,Failed_Txn_Count,Failed_Txn_Amount,First_Successful_Date,Last_successful_Date,First_Attempted_Date,Last_Attempted_Date,First_Investment_Date,Last_Investment_Date,(case when (Last_investment_date>Test_withdrawal_date)  or (Test_withdrawal_date is null and First_investment_date is not null ) then current_date else Test_withdrawal_date end) as Last_Withdrawal_Date from   (select  member_id as MemberId, cast(coalesce(sum(case when status=4 and process_status=1 and transaction_type=0 then amount end),0) as bigint) as Investment_Amount, count(case when status=4 and process_status=1 and transaction_type=0 then member_id end) as Investment_Count, coalesce(sum(case when status=4 and transaction_type=1  and  process_status=1 and lower(redemption_status) in ('success') then amount end),0) as Withdrawal_Amount, count(case when status=4 and transaction_type=1  and process_status=1 and lower(redemption_status) in ('success') then member_id end) as Withdrawal_Count, count(case when transaction_type=0 then member_id end) as Total_Attempted_Investment_Transaction, coalesce(count(case when status=4 and process_status=1 then member_id end),0) as Success_Txn_Count, coalesce(cast(sum(case when status=4 and process_status=1 then amount end) as bigint),0) as Success_Txn_Amount, coalesce(count(case when status=5 then member_id end),0) as Failed_Txn_Count, cast(coalesce(sum(case when status=5 then amount end),0) as bigint)  as Failed_Txn_Amount, min(case when status=4 and process_status=1 then updated_at end) as First_Successful_Date, max(case when status=4  and process_status=1 then updated_at end) as Last_successful_Date, min(updated_at) as First_Attempted_Date, max(updated_at) as Last_Attempted_Date, min(case when status=4 and process_status=1 and transaction_type=0 then updated_at end) as First_Investment_Date, max(case when status=4 and process_status=1 and transaction_type=0 then updated_at end) as Last_Investment_date, max(case when status=4 and process_status=1 and transaction_type=1 and lower(redemption_status) in ('success') then updated_at end) as Test_withdrawal_date from p2p_investments.p2ptransactions where day>='20220515' group by 1)) a  left join (select a.partner_user_id,current_investment,b.member_id from p2p_investments.p2puserportfolio a left join p2p_investments.p2pusers b on a.partner_user_id=b.partner_user_id where date(a.created_at)=date_add('Day',-1,current_date) and current_investment>=1) b on a.MemberId=member_id" 
  print(query2)
  #logging.inf("Query2: {}".format(query2))
  session = boto3.Session()
  client = session.client('athena',region_name='ap-south-1') 
  bucket_name='mbk-athena-temp'
  location='athena-cli-results/output'
  s3_loc='s3://' + bucket_name + '/' + location+'/'
  query_executed=client.start_query_execution(QueryString = query1, ResultConfiguration = {'OutputLocation': s3_loc})
  execution_id=query_executed['QueryExecutionId']
  if check_query_status(client, execution_id, 60) is True:
    query_executed=client.start_query_execution(QueryString = query2, ResultConfiguration = {'OutputLocation': s3_loc})
    execution_id=query_executed['QueryExecutionId']
    if check_query_status(client, execution_id, 1200) is True:
      return
  else:
    logger.info("Query failed, terminating Task..")
  #TODO Terminate

def delete_old_data():
    #s3://mbk-datalake-common-prod/lending_schema/lending_xtra_vars
  s3_client = boto3.client('s3')
  BUCKET = 'mbk-datalake-common-prod'
  PREFIX = 'lending_schema/lending_xtra_vars/'
  response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
  if 'Contents' in response:
    for object in response['Contents']:
      print('Deleting', object['Key'])
      s3_client.delete_object(Bucket=BUCKET, Key=object['Key'])
  else:
    print("Old data not found..")






def query_out_clean(bucket_name, location, query_id):
  s3 = boto3.resource('s3')
  s3.Object(bucket_name, location+"/"+query_id+".txt").delete()
  s3.Object(bucket_name, location+"/"+query_id+".metadata").delete()


def check_query_status(client, query_id, wait_time_sec):
  iterations=wait_time_sec/5
  while (iterations > 0):
    iterations = iterations - 1
    response_get_query_details = client.get_query_execution(QueryExecutionId = query_id)
    status = response_get_query_details['QueryExecution']['Status']['State']
    print("Execution ID: {}, Status is {}".format(query_id, status))
    if (status == 'FAILED') or (status == 'CANCELLED'):
      logger.info("Query Id: {} Failed".format(query_id))
      logger.info("Query response is: {}".format(response_get_query_details['QueryExecution']))
      return False
    elif status == 'SUCCEEDED':
      return True
    else:
      time.sleep(5)

#create_load_athena()




local_tz = pendulum.timezone("Asia/Kolkata")

dag = DAG(
    dag_id='20221209_kk_dag1',
    default_args=args,
    schedule_interval='20 4 * * *',
    start_date=datetime(2022,11,1,tzinfo=local_tz),
    dagrun_timeout=timedelta(minutes=120),
    tags=['replication','mongo','mysql'],
    catchup=True
    )



load_athena_table=PythonOperator(dag=dag, task_id='20221209_kk_athena1', python_callable=self.create_load_athena)

load_athena_table




--------------------



def create_load_athena():
  #TODO Delete existing files
  delete_old_data()
  query1="CREATE EXTERNAL TABLE if not exists lending_schema.lending_xtra_vars(memberid string , investment_amount bigint , investment_count bigint , withdrawal_amount double , withdrawal_count bigint , total_attempted_investment_transaction bigint , success_txn_count bigint , success_txn_amount bigint , failed_txn_count bigint , failed_txn_amount bigint , first_successful_date timestamp , last_successful_date timestamp , first_attempted_date timestamp , last_attempted_date timestamp , first_investment_date timestamp , last_investment_date timestamp , last_withdrawal_date timestamp , current_investment double , current_investment_bucket varchar(6) , day_difference bigint , day_diff_bucket varchar(4) )ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' location 's3://mbk-datalake-common-prod/lending_schema/lending_xtra_vars/'"
  print(query1)
  #logging.inf("Query1: {}".format(query1))
  query2="insert into lending_schema.lending_xtra_vars  select a.*,b.current_investment as Current_Investment,case when cast(current_investment as decimal(18,2))<=1000 then '0-1000' when cast(current_investment as decimal(18,2))>1000 and cast(current_investment as decimal(18,2)) is not null then '>1000' end as Current_Investment_Bucket ,date_diff('day',date(First_investment_date),date(Last_Withdrawal_Date)) as Day_Difference, case when date_diff('day',date(First_investment_date),date(Last_Withdrawal_Date))<=15 then '0-15' when (date_diff('day',date(First_investment_date),date(Last_Withdrawal_Date))>15 and date_diff('day',date(First_investment_date),date(Last_Withdrawal_Date)) is not null) then '>15' end as Day_Diff_Bucket  from (  select MemberId,Investment_Amount,Investment_Count,Withdrawal_Amount,Withdrawal_Count,Total_Attempted_Investment_Transaction,Success_Txn_Count,Success_Txn_Amount,Failed_Txn_Count,Failed_Txn_Amount,First_Successful_Date,Last_successful_Date,First_Attempted_Date,Last_Attempted_Date,First_Investment_Date,Last_Investment_Date,(case when (Last_investment_date>Test_withdrawal_date)  or (Test_withdrawal_date is null and First_investment_date is not null ) then current_date else Test_withdrawal_date end) as Last_Withdrawal_Date from   (select  member_id as MemberId, cast(coalesce(sum(case when status=4 and process_status=1 and transaction_type=0 then amount end),0) as bigint) as Investment_Amount, count(case when status=4 and process_status=1 and transaction_type=0 then member_id end) as Investment_Count, coalesce(sum(case when status=4 and transaction_type=1  and  process_status=1 and lower(redemption_status) in ('success') then amount end),0) as Withdrawal_Amount, count(case when status=4 and transaction_type=1  and process_status=1 and lower(redemption_status) in ('success') then member_id end) as Withdrawal_Count, count(case when transaction_type=0 then member_id end) as Total_Attempted_Investment_Transaction, coalesce(count(case when status=4 and process_status=1 then member_id end),0) as Success_Txn_Count, coalesce(cast(sum(case when status=4 and process_status=1 then amount end) as bigint),0) as Success_Txn_Amount, coalesce(count(case when status=5 then member_id end),0) as Failed_Txn_Count, cast(coalesce(sum(case when status=5 then amount end),0) as bigint)  as Failed_Txn_Amount, min(case when status=4 and process_status=1 then updated_at end) as First_Successful_Date, max(case when status=4  and process_status=1 then updated_at end) as Last_successful_Date, min(updated_at) as First_Attempted_Date, max(updated_at) as Last_Attempted_Date, min(case when status=4 and process_status=1 and transaction_type=0 then updated_at end) as First_Investment_Date, max(case when status=4 and process_status=1 and transaction_type=0 then updated_at end) as Last_Investment_date, max(case when status=4 and process_status=1 and transaction_type=1 and lower(redemption_status) in ('success') then updated_at end) as Test_withdrawal_date from p2p_investments.p2ptransactions where day>='20220515' group by 1)) a  left join (select a.partner_user_id,current_investment,b.member_id from p2p_investments.p2puserportfolio a left join p2p_investments.p2pusers b on a.partner_user_id=b.partner_user_id where date(a.created_at)=date_add('Day',-1,current_date) and current_investment>=1) b on a.MemberId=member_id" 
  print(query2)
  #logging.inf("Query2: {}".format(query2))
  session = boto3.Session()
  client = session.client('athena',region_name='ap-south-1') 
  bucket_name='mbk-athena-temp'
  location='athena-cli-results/output'
  s3_loc='s3://' + bucket_name + '/' + location+'/'
  query_executed=client.start_query_execution(QueryString = query1, ResultConfiguration = {'OutputLocation': s3_loc})
  execution_id=query_executed['QueryExecutionId']
  if check_query_status(client, execution_id, 60) is True:
    query_executed=client.start_query_execution(QueryString = query2, ResultConfiguration = {'OutputLocation': s3_loc})
    execution_id=query_executed['QueryExecutionId']
    if check_query_status(client, execution_id, 1200) is True:
      return
  else:
    logger.info("Query failed, terminating Task..")
  #TODO Terminate

def delete_old_data():
    #s3://mbk-datalake-common-prod/lending_schema/lending_xtra_vars
  s3_client = boto3.client('s3')
  BUCKET = 'mbk-datalake-common-prod'
  PREFIX = 'lending_schema/lending_xtra_vars/'
  response = s3_client.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)
  if 'Contents' in response:
    for object in response['Contents']:
      print('Deleting', object['Key'])
      s3_client.delete_object(Bucket=BUCKET, Key=object['Key'])
  else:
    print("Old data not found..")






def query_out_clean(bucket_name, location, query_id):
  s3 = boto3.resource('s3')
  s3.Object(bucket_name, location+"/"+query_id+".txt").delete()
  s3.Object(bucket_name, location+"/"+query_id+".metadata").delete()


def check_query_status(client, query_id, wait_time_sec):
  iterations=wait_time_sec/5
  while (iterations > 0):
    iterations = iterations - 1
    response_get_query_details = client.get_query_execution(QueryExecutionId = query_id)
    status = response_get_query_details['QueryExecution']['Status']['State']
    print("Execution ID: {}, Status is {}".format(query_id, status))
    if (status == 'FAILED') or (status == 'CANCELLED'):
      logger.info("Query Id: {} Failed".format(query_id))
      logger.info("Query response is: {}".format(response_get_query_details['QueryExecution']))
      return False
    elif status == 'SUCCEEDED':
      return True
    else:
      time.sleep(5)
