__author__ = 'ketankk'

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import date_format
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DoubleType, FloatType, DateType
from pyspark.sql.types import *
from awsglue.transforms import *
import boto3
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
#BooleanType, TimestampType, StringType, IntegerType, StructType, StructField, DoubleType, FloatType, DateType, StringType, IntegerType, StructType, StructField, DoubleType, FloatType, DateType


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

db_name = "mg_cross_sell"
table_name = "policy"
p_key = "obj_id"
audit_col = "last_modified"
insert_type = "upsert"
part_col = "day"
#source_path = f"s3://mbk-nifi-landingzone/data/mg_cross_sell/policy/"
#source_path = f"s3://mbk-nifi-landingzone/data/{db_name}/{table_name}/15926e56-7d81-4572-8cd3-354e74b60296.parquet"

source_path = f"s3://mbk-nifi-landingzone/data/{db_name}/{table_name}/"

source_path_dyf = {"paths": [source_path]}


additional_options = {
    "hoodie.table.name": table_name,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": insert_type,
    "hoodie.datasource.write.recordkey.field": p_key,
    "hoodie.datasource.write.precombine.field": audit_col,
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.parquet.compression.codec": "snappy",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": db_name,
    "hoodie.datasource.hive_sync.table": table_name,
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.write.partitionpath.field": part_col,
    "hoodie.datasource.hive_sync.support_timestamp": "true"

}
# target_path=f's3://mbk-datalake-common-prod/mg_cross_sell/policy/'



s3 = boto3.resource('s3')
s3paths = []
bucket = s3.Bucket(name="mbk-nifi-landingzone")
i = 0

schema = StructType([
    StructField("obj_id", StringType(), False),
    StructField("member_id", StringType(), True),
    StructField("created_at", TimestampType(), True),  # Not null, set some date
    StructField("last_modified", TimestampType(), True)  # Not null, set current date
])

df = spark.createDataFrame([], schema)
df.printSchema()
db_name = "mg_cross_sell"
table_name = "policy"
p_key = "obj_id"
audit_col = "last_modified"
insert_type = "upsert"

df = spark.createDataFrame([], schema)
for obj in bucket.objects.filter(Prefix="data/mg_cross_sell/policy/"):
    file = obj.key

    s3_path = f"s3://mbk-nifi-landingzone/{file}"
    # print(s3_path)
    # s3paths.append(s3_path)
    source_path_dyf = {"paths": [s3_path]}
    try:
        src_data = glueContext.create_dynamic_frame.from_options( \
            connection_type="s3", \
            connection_options=source_path_dyf, \
            format="parquet").toDF()
        src_data = src_data.withColumn("insurance_sell_platform",
                                       src_data["insurance_sell_platform"].cast(StringType()))
        src_data = src_data.withColumn("cross_sell", src_data["cross_sell"].cast(BooleanType()))
        src_data = src_data.withColumn("debited_at", src_data["debited_at"].cast(TimestampType()))
        df = df.union(src_data.select("obj_id", "member_id", "last_modified", "created_at"))  # use all columns
    except Exception as e:
        print("Exception!!")
        print(e)
    print(f"Counter is: {i}")
    i = i + 1

from pyspark.sql.functions import date_format

df = df.withColumn("day", date_format("created_at", "yyyyMMdd"))

df.printSchema()
# print(s3paths)
# df.show(3)
# print(df.count())
target_path = f's3://mbk-datalake-common-prod/mg_cross_sell/policy/'
df.write.format("org.apache.hudi").options(**additional_options).mode("append").save(target_path)
