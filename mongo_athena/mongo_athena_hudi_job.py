__author__ = 'ketankk'

import sys

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions

from awsglue.transforms import *
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField, DoubleType, \
    StringType, TimestampType
from pyspark.sql.functions import date_format
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
import json

sc = SparkContext.getOrCreate()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# TODO:
#  1. Get parameters as JSON
#  2. Read double type
#  3. Read Timestamp type
#  4. Source path, destination path
#  5. HUDI parameters
#  6. Delete landing zone data
# 2.
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'params'])

log = glueContext.get_logger()
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

# spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
spark.conf.set("spark.sql.parquet.int96TimestampConversion", "true")

# df = df.withColumn("time_formatted", date_format(to_timestamp("etaTs", "yyyy/MM/dd HH:mm:ss"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))....$ spark-shell

# spark.conf.set("spark.sql.parquet.int96TimestampConversion", "true")

params = json.loads(args['params'])
print(params)
# params = args['params']
log.info(f"Params are: {params}")
# {"double_col_list": ["amount_paid"], "ts_col_list": ["booked_at", "debited_at", "created_at", "last_modified"], "landing_bucket": "mbk-nifi-landingzone", "data_path": "data", "target_bucket": "mbk-datalake-common-prod", "target_db_name": "mg_cross_sell", "target_table_name": "policy", "p_key": "obj_id", "audit_col": "last_modified", "insert_type": "upsert", "partition_src_col": "created_at", "partition_col": "day" }

# https://catalog.us-east-1.prod.workshops.aws/workshops/976050cc-0606-4b23-b49f-ca7b8ac4b153/en-US/600/615-glue-hudi-lab
double_col_list = params['double_col_list']  # ["amount_paid"]
ts_col_list = params['ts_col_list']  # ["booked_at", "debited_at", "created_at", "last_modified"]
# params = {'double_col_list': double_col_list, 'ts_col_list': ts_col_list}

spark = SparkSession.builder.getOrCreate()
# spark = glueContext.spark_session

# TODO Construct dynamically
schema = StructType([StructField("obj_id", StringType(), True),
                     StructField("member_id", StringType(), True),
                     StructField("insurance_category", StringType(), True),
                     StructField("insurance_sell_platform", StringType(), True),
                     StructField("insurer", StringType(), True),
                     StructField("amount_paid", DoubleType(), True),
                     StructField("cross_sell", BooleanType(), True),
                     StructField("booked_at", TimestampType(), True),
                     StructField("status", StringType(), True),
                     StructField("sum_assured", StringType(), True),
                     StructField("tenure_months", StringType(), True),
                     StructField("policy_ref", StringType(), True),
                     StructField("customer_state", StringType(), True),
                     StructField("customer_pincode", StringType(), True),  # Data conversion was failing
                     StructField("valid_from", TimestampType(), True),
                     StructField("valid_upto", TimestampType(), True),
                     StructField("debited_at", TimestampType(), True),
                     StructField("created_at", TimestampType(), True),
                     StructField("last_modified", TimestampType(), True)])

pp = {"double_col_list": ["amount_paid"], "ts_col_list": ["booked_at", "debited_at", "created_at", "last_modified"]}
sfList = []

for col in params.get("double_col_list"):
    sf = StructField(col, DoubleType(), True)
    sfList.append(sf)

for col in params.get("ts_col_list"):
    sf = StructField(col, TimestampType(), True)
    sfList.append(sf)

schema = StructType(sfList)

landing_bucket = params['landing_bucket']  # "mbk-nifi-landingzone"
data_path = params['data_path']  # "data"

target_bucket = params['target_bucket']  # "mbk-datalake-common-prod"
target_db_name = params['target_db_name']  # "mg_cross_sell"
target_table_name = params['target_table_name']  # "policy"

p_key = params['p_key']  # "obj_id"  # primary_keys
audit_col = params['audit_col']  # "last_modified"
insert_type = params['insert_type']  # "upsert"

partition_src_col = params['partition_src_col']  # "created_at"
partition_col = params['partition_col']  # "day"
print(landing_bucket, data_path, target_bucket, target_db_name, target_table_name, p_key, audit_col, insert_type,
      partition_src_col, partition_col)
# TODO update table name
source_path = f"s3://{landing_bucket}/{data_path}/{target_db_name}/{target_table_name}/"

target_path = f"s3://{target_bucket}/{target_db_name}/{target_table_name}/"

log.info(f"Source Path is: {source_path}")
log.info(f"Target Path is: {target_path}")

new_data = spark.read.format("parquet") \
    .schema(schema) \
    .load(source_path)

log.info("Data Schema is:")
new_data.printSchema()

data_count = new_data.count()
log.info(f"Data loaded, data size is: {data_count}")

new_data = new_data.withColumn(partition_col, date_format(partition_src_col, "yyyyMMdd"))
log.info("Partition column added, schema->")
new_data.printSchema()

# Common HUDI params
hudi_params = {
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.parquet.compression.codec": "snappy",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.support_timestamp": "true",
    "hoodie.schema.on.read.enable": "true",
    "hoodie.datasource.write.reconcile.schema": "true",  # For handling schema changes
    # "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.DefaultHoodieRecordPayload" #An error occurred while calling o158.save. cannot modify hudi meta col: _hoodie_commit_time
}
# Table specific HUDI parameters
hudi_table_config = {
    "hoodie.table.name": target_table_name,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": insert_type,
    "hoodie.datasource.write.recordkey.field": p_key,  # Comma separated values
    "hoodie.datasource.write.precombine.field": audit_col,
    "hoodie.datasource.hive_sync.database": target_db_name,
    "hoodie.datasource.hive_sync.table": target_table_name,
    "hoodie.datasource.write.partitionpath.field": partition_col,
}

hudi_params.update(hudi_table_config)

log.info("HUDI Parameters ")
# This is a key-value based dictionary so using print statement for log
log.info(print(*hudi_params.items(), sep='\n'))

new_data.write.format("org.apache.hudi") \
    .options(**hudi_params) \
    .mode("append") \
    .save(target_path)

print('done')

# df = df.withColumn("time_formatted", date_format(to_timestamp("etaTs", "yyyy/MM/dd HH:mm:ss"),"yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
