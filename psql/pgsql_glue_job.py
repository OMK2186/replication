__author__ = 'ketankk'

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, desc
from awsglue.dynamicframe import DynamicFrame

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

logger = glueContext.get_logger()

args = getResolvedOptions(sys.argv, ['JOB_NAME', glue_params])
glue_params = args["glue_params"]

logger.info(f"Glue Parameters are: {glue_params} ")

landing_bucket = glue_params["landing_bucket"]  #: "mbk-nifi-landingzone",
data_path = glue_params["data_path"]  #: "data",
target_bucket = glue_params["target_bucket"]  #: "mbk-datalake-common-prod",
target_db = glue_params["target_db"]  #: "pg_merchant",
target_table = glue_params["target_table"]  #: "mprofile_merchantprofile",
pk_list = glue_params["pk_list"]  #:Array of primary key ["id"],
load_type = glue_params["load_type"]  #: "FULL","CDC", "INCREMENTAL"
audit_col = glue_params["audit_col"]  #: updated_at,
partition_col = glue_params["partition_col"]  # created_at null

source_path = f"s3://{landing_bucket}/{data_path}/{target_db}/{target_table}/"
source_path_dyf = {"paths": [source_path]}
src_data = glueContext.create_dynamic_frame.from_options( \
    connection_type="s3", \
    connection_options=source_path_dyf, \
    format="parquet" \
    ).toDF()



# window = Window.partitionBy(pk_list).orderBy(col(table_dict['AuditCol'].desc())
window = Window.partitionBy(pk_list).orderBy(col('id').desc())

deduped_data = src_data.select("*", row_number().over(window).alias("r_num")).filter(col("r_num") == 1).drop("r_num")
deduped_data.count()

additional_options = {
    "hoodie.table.name": "mprofile_transactiontemplate",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": "insert_overwrite",
    "hoodie.datasource.write.recordkey.field": "id",
    #    "hoodie.datasource.write.precombine.field": "UpdatedAt",
    #   "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.parquet.compression.codec": "snappy",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "pg_merchant",
    "hoodie.datasource.hive_sync.table": "mprofile_transactiontemplate",
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
}
target_path = 's3://mbk-datalake-common-prod/pg_merchant/mprofile_transactiontemplate/'
deduped_data.show()
deduped_data.write.format("org.apache.hudi").options(**additional_options).mode(
    "append"
).save(target_path)
