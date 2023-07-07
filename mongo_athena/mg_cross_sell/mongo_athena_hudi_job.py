__author__ = 'ketankk'

from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, StructType, StructField, DoubleType, \
    FloatType, DateType, StringType, TimestampType
from pyspark.sql.functions import date_format
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import lit
from pyspark.sql import Row
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, row_number, rank, desc, asc
from pyspark.sql.window import Window
import sys
import logging as log

glueContext = GlueContext(SparkContext.getOrCreate())

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

params = args['params']
# https://catalog.us-east-1.prod.workshops.aws/workshops/976050cc-0606-4b23-b49f-ca7b8ac4b153/en-US/600/615-glue-hudi-lab
double_col_list = params['double_col_list']  # ["amount_paid"]
ts_col_list = params['ts_col_list']  # ["booked_at", "debited_at", "created_at", "last_modified"]

# double_col_list = ["amount_paid"]
# ts_col_list = ["booked_at", "debited_at", "created_at", "last_modified"]
# params = {'double_col_list': double_col_list, 'ts_col_list': ts_col_list}

log.info(f"Params are: {params}")
spark = glueContext.spark_session

params = {"double_col_list": ["amount_paid"], "ts_col_list": ["booked_at", "debited_at", "created_at", "last_modified"], "landing_bucket": "mbk-nifi-landingzone", "data_path": "data", "target_bucket": "mbk-datalake-common-prod", "target_db_name": "mg_cross_sell", "target_table_name": "policy", "p_key": "obj_id", "audit_col": "last_modified", "insert_type": "upsert", "partition_src_col": "created_at", "partition_col": "day" }


# TODO Construct dynamically
schema = StructType([StructField("obj_id", StringType(), True),
                     StructField("member_id", StringType(), True),
                     StructField("insurance_category", StringType(), True),
                     StructField("insurance_sell_platform", StringType(), True),
                     StructField("insurer", StringType(), True),
                     StructField("amount_paid", DoubleType(), True),
                     StructField("cross_sell", BooleanType(), True),
                     StructField("booked_at", TimestampType(), True),
                     StructField("debited_at", TimestampType(), True),
                     StructField("created_at", TimestampType(), True),
                     StructField("last_modified", TimestampType(), True)])

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

hudi_params = {
    "hoodie.table.name": target_table_name,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": insert_type,
    "hoodie.datasource.write.recordkey.field": p_key,
    "hoodie.datasource.write.precombine.field": audit_col,
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.parquet.compression.codec": "snappy",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": target_db_name,
    "hoodie.datasource.hive_sync.table": target_table_name,
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.hive_sync.support_timestamp": "true", #TO dispaly time in datetime format not in timestamp format
    "hoodie.datasource.write.partitionpath.field": partition_col
}

log.info("HUDI Parameters ")
log.info(hudi_params)

new_data.write.format("org.apache.hudi") \
    .options(**hudi_params) \
    .mode("append") \
    .save(target_path)

print('done')
