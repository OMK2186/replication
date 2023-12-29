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
from pyspark.sql.functions import col, row_number, rank, desc
from pyspark.sql.window import Window
import pytz
from datetime import datetime,timedelta
from pyspark.sql.functions import lit
import os


glueContext = GlueContext(SparkContext.getOrCreate())

# spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

spark = glueContext.spark_session
logger = glueContext.get_logger()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
spark.conf.set("spark.sql.caseSensitive","false")
args = getResolvedOptions(sys.argv,
                          ['JOB_NAME',
                           'table_metafile',
                           ])

logger.info('Param read Started')
table_meta = spark.read.format('csv').option('delimiter', '|').load(args['table_metafile'])
table_args = table_meta.rdd.map(lambda x: (x))
table_dict = table_args.collectAsMap()
logger.info('Param read Completed')

# source_path = "s3://"+table_dict['SourceBucket']+"/"+table_dict['RawPath']+"/"+table_dict['Schema']+"/"+table_dict['Table']+"/*/"
source_path = "s3://" + table_dict['SourceBucket'] + "/" + table_dict['RawPath'] + "/" + table_dict['Schema'] + "/" + \
              table_dict['Table'] + "/"
target_path = "s3://" + table_dict['TargetBucket'] + "/" + table_dict['Schema'] + "/" + \
              table_dict['Table'] + "/"
source_path_dyf = {"paths": [source_path]}
target_path_dyf = {"paths": [target_path]}
athenaClient = boto3.client('athena', region_name='ap-south-1')
s3 = boto3.resource('s3')
query_string = 'MSCK repair table ' + table_dict['Schema'] + '.' + table_dict['Table']
pk_list = [pk_col for pk_col in table_dict['PrimaryKey'].split(',')]
tz = pytz.timezone('Asia/Kolkata')

today_partition = int((datetime.now(tz=tz) - timedelta(days=1)).strftime('%Y%m%d'))
yesterday_partition = int((datetime.now(tz=tz) - timedelta(days=2)).strftime('%Y%m%d'))

def athena_query(athenaClient, queryString):
    try:
        response = athenaClient.start_query_execution(
            QueryString=queryString,
            QueryExecutionContext={
                'Database': table_dict['Schema']
            },
            ResultConfiguration={
                'OutputLocation': 's3://mbk-analytics/glue_msck/'
            }
        )
    except Exception as e:
        print(e)
        print('sync_insert failed')
    finally:
        s3_file_delete('mbk-analytics', 'glue_msck')
    return response


def check_for_data(bucket_name,Schema, Table):
    Prefix = Schema + "/" + Table + "/"
    # print(Prefix)
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name, \
                                  Prefix=Prefix, MaxKeys=1)
    if response['KeyCount'] > 0:
        return True
    else:
        return False


def delete_src_files():
    bucket = s3.Bucket(table_dict['SourceBucket'])
    object_deletion_path = table_dict['RawPath'] + "/" + table_dict['Schema'] + "/" + table_dict['Table'] + "/"
    objects = bucket.objects.filter(Prefix=object_deletion_path)
    objects_to_delete = [{'Key': o.key} for o in objects]
    if len(objects_to_delete):
        s3.meta.client.delete_objects(Bucket=table_dict['SourceBucket'], Delete={'Objects': objects_to_delete})


def s3_file_delete(bucket, path):
    print("----------------------------------")
    logger.info('S3 Files delete started')
    print('Cleaning s3 files from ' + path)
    s3.Bucket(bucket).objects.filter(Prefix=path).delete()
    logger.info('S3 Files delete completed')
    print("----------------------------------")


def customUnion(df1, df2):
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


def read_source_data(dedupe):
    # src_result = check_df_size(source_path_dyf)
    try:
        #src_result = check_for_data(table_dict['Schema'], table_dict['Table'])
        logger.info('New data read started')
        #src_data = glueContext.create_dynamic_frame.from_options( \
        #    connection_type="s3", \
        #    connection_options=source_path_dyf, \
        #    format="parquet" \
        #    ).toDF()
        src_data=spark.sql('select * from {0}.{1}'.format(table_dict['TempSchema'],table_dict['TempTable']))
        logger.info('New data read Completed')
        if not src_data.rdd.isEmpty():
            delta_day = src_data
            #delta_day=delta_day.fillna(20000101, subset=['day'])
            if bool(table_dict.get('TimestampCol')):
                delta_day_time = delta_day.select(
                    *(col(c).cast("timestamp").alias(c) if c in table_dict['TimestampCol'].split(',') else c for c in
                      delta_day.columns))
            else:
                delta_day_time = delta_day
            if bool(table_dict.get('DecimalCol')):
                delta_day_final = delta_day_time.select(
                    *(col(c).cast("double").alias(c) if c in table_dict['DecimalCol'].split(',') else c for c in
                      delta_day_time.columns))
            else:
                delta_day_final = delta_day_time
            if dedupe == True:
                logger.info('Dedupe Started')
                window = Window.partitionBy(pk_list).orderBy(col(table_dict['AuditCol']).desc())
                delta_day_double = delta_day_final.select("*", row_number().over(window).alias("r_num")).filter(
                    col("r_num") == 1).drop("r_num")
                logger.info('Dedupe completed')
                return delta_day_double
            logger.info('Read Source data completed')
            return delta_day_final
        else:
            logger.info("No new files in raw location")
            return None
            # print(e)
            # exit(1)
    except Exception as e:
        raise e
        logger.info(e)
        print('sync_insert failed')

def sync_insert():
    # print()
    try:
        if table_dict['SyncType'].lower() == 'full':
            #s3_file_delete(table_dict['TargetBucket'],
            #               table_dict['FinalPath'] + "/" + table_dict['Schema'] + "/" + table_dict['Table'])
            #pass
            glueContext.purge_s3_path(target_path,{"retentionPeriod":0})
        if table_dict['SyncType'].lower() == 'cdc':
            dedupe = True
        else:
            dedupe = False
        delta_day_double = read_source_data(dedupe)
        if delta_day_double is None:
            return None
        delta_day_double = delta_day_double.repartition(table_dict['PartitionCol'])
        target_dyf = DynamicFrame.fromDF(delta_day_double, glueContext, "target_dyf")
        #logger.info(target_dyf.show(1))
        logger.info('Repartition Start')
        logger.info('Repartition Completed')
        logger.info('Write Final Data Started')
        glueContext.write_dynamic_frame.from_options( \
            frame=target_dyf, \
            connection_type="s3", \
            connection_options={"path": target_path, "partitionKeys": [table_dict['PartitionCol']]}, \
            format="parquet")
        logger.info('Write Final Data completed')
        # execution = athena_query(athenaClient, query_string)
        #s3_file_delete(table_dict['SourceBucket'],
        #               table_dict['RawPath'] + "/" + table_dict['Schema'] + "/" + table_dict['Table'])
        glueContext.purge_s3_path(source_path,{"retentionPeriod":0})
    except Exception as e:
        raise e
        logger.info(e)
        print('sync_insert failed')


def sync_cdc():
    try:
        final_data = check_for_data(table_dict['TargetBucket'],table_dict['Schema'], table_dict['Table'])
        # final_data = check_df_size(target_path_dyf)
        if not final_data:
            sync_insert()
            return None
        dedupe = True
        delta_day_double = read_source_data(dedupe)
        if delta_day_double is None:
            return None
        #new_day = spark.sql('select max(day) from {0}.{1}'.format(table_dict['Schema'],table_dict['Table'])).collect()
        #output = [i[0] for i in new_day]
        #output = str(tuple(output)).rstrip(',)') + ')'
        pushdownpredicate = "(day=='" + str(yesterday_partition) + "')"
        logger.info('Read Existing Data - New Day partition '+pushdownpredicate)
        final_data = glueContext.create_dynamic_frame.from_catalog(database=table_dict['Schema'],
                                                                   table_name=table_dict['Table'],
                                                                   push_down_predicate=pushdownpredicate).toDF()
        #final_data = spark.sql("select * from mobinew.memberbalance where day='{0}'".format(yesterday_partition))
        logger.info('Read Existing Data Complete')
        logger.info('Pushdownpredicate is {0}'.format(pushdownpredicate))
        if not final_data.rdd.isEmpty():
            logger.info('Data Merge Started')
            #merged_data = final_data.join(delta_day_double, pk_list, "left_anti").union(delta_day_double)
            #custom union
            old_data = final_data.join(delta_day_double, pk_list, "left_anti")
            merged_data = customUnion(old_data,delta_day_double)
            logger.info('Data Merge Completed')
        else:
            merged_data = delta_day_double
        logger.info('Write Final Data Started')
        #merged_data.repartition(table_dict['PartitionCol']).write.mode('overwrite').format('parquet').partitionBy(
        #    table_dict['PartitionCol']).save(target_path)
        #final_target_path=target_path+str(today_partition)+'/'
        logger.info(merged_data.printSchema())
        merged_data=merged_data.withColumn('day', lit(int(today_partition)))
        logger.info(merged_data.printSchema())
        merged_data.repartition(20).write.mode('overwrite').format('parquet').partitionBy(table_dict['PartitionCol']).save(target_path)
        logger.info('Write Final Data completed')
        # execution = athena_query(athenaClient, query_string)
        #s3_file_delete(table_dict['SourceBucket'],
        #               table_dict['RawPath'] + "/" + table_dict['Schema'] + "/" + table_dict['Table'])
        os.system(''' aws s3 mv {0} s3://mbk-data-backup/memberbalance_edits_backup/{1}/day={2}/ --recursive'''.format(source_path,table_dict['Table'],today_partition))
        #glueContext.purge_s3_path(source_path,{"retentionPeriod":0})
    except Exception as e:
        raise e
        logger.info(e)
        print('sync_cdc failed')


# read_source_data()
try:
    if table_dict['SyncType'].lower() == 'cdc':
        sync_cdc()
    elif table_dict['SyncType'].lower() == 'insert':
        sync_insert()
    elif table_dict['SyncType'].lower()== 'full':
        sync_insert()
    else:
        print('Sync type is not defined')
except Exception as e:
        raise e
        logger.info(e)
        print('Glue sync failed')
