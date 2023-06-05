mysql -u analytics -p -h common-mysql-prod-replica.cozdxcykxnil.ap-south-1.rds.amazonaws.com -P3308 -Demail_db -A

FpC35A5rMDkG7U

token_data


select distinct member_id from Demail_db.token_data;


mysql_config_editor set --login-path=email_db_an --host="common-mysql-prod-replica.cozdxcykxnil.ap-south-1.rds.amazonaws.com" --user="analytics" --port=3308 --password

FpC35A5rMDkG7U
###################


ex_day=$1
ex_day='2022-12-15'
echo "Execution date is: "$ex_day
echo "Running for last 30 days" 

outfile="/tmp/de_email_db_token_data.csv"
echo "Output location is: "$outfile
mysql --login-path=email_db_an -N -e "SELECT id, member_id, user_email, name, source, cast(token_active as signed integer) as token_active, created_at, last_updated_at, date_format(created_at,'%Y%m%d') as day FROM email_db.token_data WHERE last_updated_at >=DATE_SUB('$ex_day', interval 30 day) "> $outfile
if [ $? -ne 0 ]
then
	echo "Data extraction from MySQL failed.."
	exit 1
fi


count=`(wc -l <$outfile)`

echo "Data extracted, Row count is "$count
s3_loc="s3://mbk-datalake-common-prod/mobikwik_schema/tmp_token_data"
create_temp="CREATE EXTERNAL TABLE IF NOT EXISTS mobikwik_schema.tmp_token_data(id int, member_id string, user_email string, name string, source string, token_active int, created_at timestamp, last_updated_at timestamp, day int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '$s3_loc'" 
athenacli -e "$create_temp"
echo "Temp table created.."$create_temp

aws s3 cp $outfile $s3_loc'/de_email_db_token_data.csv'
echo "Data copied to S3"

dedupe_query="select * from email_db.token_data_1 td join mobikwik_schema.tmp_token_data ttd on td.id=ttd.id"


[data-eng-new@data-analytics-common-nifi-prod-01 centos]$ sh /apps/cron/Karan/bureau_variables/copy_data.sh 2023-01-04 20230104

mysql --login-path=email_db_an -e "SELECT id, member_id, user_email, name, source, cast(token_active as signed integer) as token_active, created_at, last_updated_at, date_format(created_at,'%Y%m%d') as day FROM email_db.token_data WHERE last_updated_at >=DATE_SUB('$ex_day', interval 30 day) "> $outfile


==========
glueContext.write_dynamic_frame.from_options(
    frame=dynamicFrame,
    connection_type="s3",
    connection_options={"path": "s3://s3path"},
    format="csv",
    format_options={
        "quoteChar": -1,
    },
)

  src_data = glueContext.create_dynamic_frame.from_options( \
        connection_type="s3", \
        connection_options=source_path_dyf, \
        format="csv", \
        format_options={\
        "separator": "\t",
        "withHeader": True
            
        }\
        ).toDF()
  


==============
##First time queries..
CREATE EXTERNAL TABLE IF NOT EXISTS email_db.token_data_1(id int, member_id string, user_email string, name string, source string, token_active int, created_at timestamp, last_updated_at timestamp) partitioned by (day int) 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mbk-datalake-common-prod/email_db/token_data_1'

===========

ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'

#aws s3 

id, member_id, user_email, name, source, cast(token_active as signed integer) as token_active, created_at, last_updated_at


select cast(token_active as signed integer) FROM email_db.token_data limit 10

aws athena start-query-execution --query-string $query2 --result-configuration "OutputLocation"="s3://mbk-datalake-common-prod/mobikwik_schema/tmp_token_data2'" --query-execution-context Database=mobinew