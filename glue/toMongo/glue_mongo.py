import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# df = glueContext.create_dynamic_frame.from_catalog(database = "hudi_mobikwik_schema", table_name = "mongo_users_profile_txns_detail", transformation_ctx = "df",additional_options = {"database":"users_profile", "collection":"txns_detail"})""

path1 = "s3://mbk-data-users/de-projects/user-per/parq/up_agg_data_097abc61-164c-4e9d-b375-73e0879e755c.parquet"

# path1="s3://mbk-data-users/de-projects/user-per/parq/"


source_path_dyf = {"paths": [path1]}

src_data = glueContext.create_dynamic_frame.from_options( \
    connection_type="s3", \
    connection_options=source_path_dyf, \
    format="parquet" \
    ).toDF()

print(src_data.schema)


def Merge(rec):
    rec['_id'] = rec['member_id']
    if rec["_id"] == "9026569317@nocash.mobikwik.com":
        rec["data"] = "Data Updated2!!"
    return rec


write_mongo_options = {"database": "users_profile", "collection": "txns_detail"}

df = glueContext.create_dynamic_frame.from_catalog(database="hudi_mobikwik_schema",
                                                   table_name="mongo_users_profile_txns_detail",
                                                   transformation_ctx="df", additional_options=write_mongo_options)

from awsglue.dynamicframe import DynamicFrame

src_data_dyn = DynamicFrame.fromDF(src_data, glueContext, "df_to_ddf")

print(src_data_dyn.schema())
src_data_dyn = src_data_dyn.map(f=Merge)
# src_data_dyn['_id']=src_data_dyn['member_id']

print("New schema")
print(src_data_dyn.schema())
print(src_data_dyn.show())

# glueContext.write_dynamic_frame.from_options(src_data_dyn, connection_type="mongodb", connection_options=write_mongo_options)

# glueContext.write_dynamic_frame.from_options(src_data_dyn, database = "hudi_mobikwik_schema", table_name = "mongo_users_profile_txns_detail", transformation_ctx = "df",additional_options = write_mongo_options)

# from_catalog

# txId = glueContext.start_transaction(read_only=False)
# glueContext.write_dynamic_frame.from_catalog(frame=src_data_dyn, database = db, table_name = tbl, transformation_ctx = "datasource0",     additional_options={"transactionId":txId})
# glueContext.commit_transaction(txId)


# glueContext.write_dynamic_frame.from_catalog(frame = src_data_dyn, database = "hudi_mobikwik_schema", table_name = "mongo_users_profile_txns_detail", transformation_ctx = "df", additional_options = write_mongo_options)

write_uri = "mongodb://10.13.0.83:27017/admin"

write_mongo_options = {
    "uri": write_uri,
    "database": "users_profile",
    "collection": "txns_detail",
    "username": "mongouser",
    "password": "fcS^15GVm81m"
}

# write_mongo_options = {"database":"users_profile", "collection":"txns_detail"}

glueContext.write_dynamic_frame.from_options(src_data_dyn, connection_type="mongodb",
                                             connection_options=write_mongo_options)

df = glueContext.create_dynamic_frame.from_catalog(database="hudi_mobikwik_schema",
                                                   table_name="mongo_users_profile_txns_detail",
                                                   transformation_ctx="df", additional_options=write_mongo_options)

print(df.schema())
print("Hello")
print(df)
# print(df.show())
print("Done Done Done....!!!!")

# glueContext.write_dynamic_frame.from_options(src_data, connection_type="mongodb", connection_options=write_mongo_options)

# df = glueContext.create_dynamic_frame.from_catalog(database = "hudi_mobikwik_schema", table_name = "mongo_users_profile_txns_detail", transformation_ctx = "df",additional_options = write_mongo_options)


job.commit()