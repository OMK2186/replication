__author__ = 'ketankk'

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from pyspark.context import SparkContext

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

log = glueContext.get_logger()

src_path = "s3://mbk-datalake-common-prod/mobinew/member_base/"
src_path2 = "s3://mbk-nifi-landingzone/data/mobinew/member_base_snapshot/day=20231130/"


def customUnion(df1, df2):
    df1 = df1.toDF(*[c.lower() for c in df1.columns])
    df2 = df2.toDF(*[c.lower() for c in df2.columns])

    cols1 = df1.columns
    cols2 = df2.columns
    total_cols = sorted(cols1 + list(set(cols2) - set(cols1)))

    def expr(mycols, allcols):
        def processCols(colname):
            if colname in mycols:
                return colname
            else:
                return lit(None).alias(colname)

        cols = map(processCols, allcols)
        return list(cols)

    appended = df1.select(expr(cols1, total_cols)).union(df2.select(expr(cols2, total_cols)))
    return appended


def check_df_size(dyf_path):
    dyf = glueContext.create_dynamic_frame.from_options( \
        connection_type="s3", \
        connection_options=dyf_path, \
        format="parquet" \
        ).toDF()
    if ((len(dyf.head(1)) > 0)):
        return dyf
    else:
        return 'Empty DF'


source_path_dyf = {"paths": [src_path]}
df1 = check_df_size(source_path_dyf)
df1 = df1.drop('day')
df1.printSchema()

df1 = df1.filter("createdat < '2023-11-30'")

source_path_dyf2 = {"paths": [src_path2]}
df2 = check_df_size(source_path_dyf2)

merged_data = customUnion(df1, df2)

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.functions import lit

ts_cols = ['createdat', 'updatedat', 'kyc_date']
double_cols = ['commission']

# 149553632
# 149551858
# 149551116
df_ts = merged_data.select(
    *(col(c.lower()).cast("timestamp").alias(c.lower()) if c.lower() in ts_cols else c.lower() for c in merged_data.columns))

df_double = df_ts.select(*(col(c).cast("double").alias(c) if c.lower() in double_cols else c for c in df_ts.columns))
df_double.printSchema()

window = Window.partitionBy(['id']).orderBy(col('updatedat').desc())

df_final = df_double.select("*", row_number().over(window).alias("r_num")).filter(col("r_num") == 1).drop("r_num")
df_final = df_final.toDF(*[c.lower() for c in df_final.columns])

day = '20231130'
df_final = df_final.withColumn('day', lit(day))

log.info("Partition column added, schema->")
df_final.printSchema()

target_db_name = 'snapshots_db'
target_table_name = 'member_base_monthly'
target_bucket = "mbk-datalake-common-prod"

target_path = f"s3://{target_bucket}/{target_db_name}/{target_table_name}/day={day}"

df_final.write.format('parquet').save(target_path)

print('Data inserted!!')
'''
CREATE EXTERNAL TABLE `snapshots_db.member_base_monthly`(
  `passcode` string, 
  `createdby` string, 
  `createdat` timestamp, 
  `statusid` int, 
  `enabled` int, 
  `maxaddons` int, 
  `commission` double, 
  `id` int, 
  `kyc_status` int, 
  `updatedat` timestamp, 
  `kyc_date` timestamp, 
  `encrypted_memberid` string, 
  `encrypted_emailid` string, 
  `encrypted_loginid` string, 
  `encrypted_primarycell` string)
PARTITIONED BY ( 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mbk-datalake-common-prod/snapshots_db/member_base_monthly'
'''