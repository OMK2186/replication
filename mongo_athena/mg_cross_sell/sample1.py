__author__ = 'ketankk'

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DoubleType, FloatType, DateType
from pyspark.sql.functions import date_format
from pyspark.sql import SparkSession, HiveContext
from pyspark.sql.functions import lit
from pyspark.sql import Row
import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, row_number, rank, desc,asc
from pyspark.sql.window import Window

glueContext = GlueContext(SparkContext.getOrCreate())

spark = glueContext.spark_session



from pyspark.sql.functions import date_format
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType


schema = StructType([StructField("obj_id", StringType(),True),
 StructField("member_id", StringType(),True),
 StructField("insurance_category", StringType(),True),
 StructField("insurance_sell_platform", StringType(),True),
 StructField("insurer", StringType(),True),
 StructField("amount_paid", DoubleType(),True),
 StructField("cross_sell", BooleanType(),True),
 StructField("booked_at", TimestampType(),True),
 StructField("debited_at", TimestampType(),True),
 StructField("created_at", TimestampType(),True),
 StructField("last_modified", TimestampType(),True)])


source_path="s3://mbk-nifi-landingzone/data/mg_cross_sell/policy/"
target_path="s3://mbk-nifi-landingzone/data/mg_cross_sell/policy3/"
source_path="s3://mbk-nifi-landingzone/data/mg_cross_sell/policy/"#9723d862-24ba-42da-b48c-f51bcee74c5a2.parquet"
df2=spark.read.format("parquet").schema(schema).load(source_path)
df2.filter("debited_at is not null").show(5)
df2.printSchema()
df2.write.parquet(target_path, mode="overwrite")


from pyspark.sql.functions import date_format
df2=df2.withColumn("day", date_format("created_at", "yyyyMMdd"))


'''
db_name="mg_cross-sell"
table_name="policy"
p_key="obj_id"
audit_col="last_modified"
insert_type="upsert"
part_col="day"

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
    "hoodie.datasource.write.partitionpath.field": part_col
}
target_path=f's3://mbk-datalake-common-prod/{db_name}/{table_name}/'

df2.write.format("org.apache.hudi").options(**additional_options).mode("append").save(target_path)


#src_data.toDF().write.format("parquet").save(target_path)
print('done')
'''

df2.withColumn("booked_at", from_unixtime($"booked_at")).
  withColumn("daysDiff", datediff($"tsDate", $"checkIn")).
  show

import org.apache.spark.sql.types.DateType

val newDF = df.withColumn("dateColumn", df("timestampColumn").cast(DateType))

from pyspark.sql.functions import *
to_date()

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('PySpark to_date()').getOrCreate()
dataframe = spark.createDataFrame(data = [ ("1","1688056596")], schema=["id","input_timestamp"])
df = spark.createDataFrame(data = [ ("1",1688056596)], schema=["id","input_timestamp"])

df.withColumn("day", df['input_timestamp'])


schema = StructType([StructField("obj_id", StringType(), True), StructField("last_modified", IntegerType(), True)])
df = spark.createDataFrame(data = [ ("1", 1688056596)], schema = schema)
df = spark.createDataFrame(data = [ ("1", None)], schema = schema)


df.withColumn("day", to_date(("last_modified"), "yyyy-MM-dd")).show()
