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
                           'type',
                           'd_day',
                           'y_day',
                           ])

#s3://mbk-nifi-landingzone/replication_v2/configs/paylater/userpaylaterbalance_closing.glue.csv

logger.info('Param read Started')
table_meta = spark.read.format('csv').option('delimiter', '|').load(args['table_metafile'])
table_args = table_meta.rdd.map(lambda x: (x))
table_dict = table_args.collectAsMap()
logger.info('Param read Completed')

# source_path = "s3://"+table_dict['source_bucket']+"/"+table_dict['raw_path']+"/"+table_dict['Schema']+"/"+table_dict['Table']+"/*/"
source_path = "s3://" + table_dict['source_bucket'] + "/" + table_dict['raw_path'] + "/" + table_dict['schema'] + "/" + \
              table_dict['table'] + "/"

target_path = "s3://" + table_dict['target_bucket'] + "/" + table_dict['schema'] + "/" + \
              table_dict['table'] + "/"


s3 = boto3.resource('s3')
query_string = 'MSCK repair table ' + table_dict['schema'] + '.' + table_dict['table']
pk_list = [pk_col for pk_col in table_dict['primary_key'].split(',')]
tz = pytz.timezone('Asia/Kolkata')

#today_partition = int((datetime.now(tz=tz) - timedelta(days=1)).strftime('%Y%m%d'))
#yesterday_partition = int((datetime.now(tz=tz) - timedelta(days=2)).strftime('%Y%m%d'))

today_partition = args['d_day']
yesterday_partition = args['y_day']

source_path = source_path + "day="+today_partition

source_path_dyf = {"paths": [source_path]}
target_path_dyf = {"paths": [target_path]}



def check_for_data(bucket_name, schema, table):
    Prefix = schema + "/" + table + "/"
    # print(Prefix)
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name, \
                                  Prefix=Prefix, MaxKeys=1)
    if response['KeyCount'] > 0:
        return True
    else:
        return False



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
    try:
        logger.info('New data read started')
       
        #src_count=spark.sql('select count(*) from {0}.{1}'.format(table_dict['temp_schema'],table_dict['temp_table']))
       
        #logger.info('New data row count {}'.format(src_count))

        src_data = glueContext.create_dynamic_frame.from_options( \
                connection_type="s3", \
                connection_options=source_path_dyf, \
                format="parquet" \
                ).toDF()
                
        logger.info('New data read Completed')
        logger.info("Decimal cols {}".format(table_dict['decimal_cols']))
        logger.info("Timestamp cols {}".format(table_dict['timestamp_cols']))
        
        if not src_data.rdd.isEmpty():
            delta_day = src_data
            if bool(table_dict.get('timestamp_cols')):
                delta_day_time = delta_day.select(
                    *(col(c).cast("timestamp").alias(c) if c.lower() in table_dict['timestamp_cols'].split(',') else c for c in
                      delta_day.columns))
            else:
                delta_day_time = delta_day
            if bool(table_dict.get('decimal_cols')):
                delta_day_final = delta_day_time.select(
                    *(col(c).cast("double").alias(c) if c.lower() in table_dict['decimal_cols'].split(',') else c for c in
                      delta_day_time.columns))
            else:
                delta_day_final = delta_day_time
            if dedupe == True:
                logger.info('Dedupe Started')
                window = Window.partitionBy(pk_list).orderBy(col(table_dict['audit_col']).desc())
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
        dedupe = True
        delta_day_double = read_source_data(dedupe)
        if delta_day_double is None:
            return None
        delta_day_double = delta_day_double.repartition(table_dict['partition_col_src'])
        target_dyf = DynamicFrame.fromDF(delta_day_double, glueContext, "target_dyf")
        #logger.info(target_dyf.show(1))
        logger.info('Repartition Start')
        logger.info('Repartition Completed')
        logger.info('Write Final Data Started')
        glueContext.write_dynamic_frame.from_options( \
            frame=target_dyf, \
            connection_type="s3", \
            connection_options={"path": target_path, "partitionKeys": [table_dict['partition_col_src']]}, \
            format="parquet")
        logger.info('Write Final Data completed')
       
        glueContext.purge_s3_path(source_path,{"retentionPeriod":0})
    except Exception as e:
        raise e
        logger.info(e)
        print('sync_insert failed')



def sync_snapshot():
    try:
        final_data = check_for_data(table_dict['target_bucket'],table_dict['schema'], table_dict['table'])
        # final_data = check_df_size(target_path_dyf)
        #if not final_data:
         #   sync_insert()
          #  return None
        
        dedupe = True
        delta_day_double = read_source_data(dedupe)
        if delta_day_double is None or delta_day_double.rdd.isEmpty():
            return None
            
        logger.info("Data fetched, schema->..{}".format(delta_day_double.schema))

        pushdownpredicate = "(day=='" + str(yesterday_partition) + "')"
        logger.info('Read Existing Data - From yesterdays partition: {}'.format(pushdownpredicate))
        
        try:
            
            #Initially Started with new data, if data exists then merge both the data
            
            merged_data = delta_day_double

            final_data = glueContext.create_dynamic_frame.from_catalog(database=table_dict['schema'],
                                                                   table_name=table_dict['table'],
                                                                   push_down_predicate=pushdownpredicate).toDF()
                                                            
            if not final_data.rdd.isEmpty():
                logger.info('Old data found, Data Merge Started')
                old_data = final_data.join(delta_day_double, pk_list, "left_anti")
                merged_data = customUnion(old_data, delta_day_double)
                logger.info('Data Merge Completed')

                                                                   
        except Exception as e:
            logger.error("Error in fetching existing data {}".format(e))


        logger.info('Write Final Data Started')
        logger.info("Existing Schema:".format(merged_data.printSchema()))
        merged_data=merged_data.withColumn('day', lit(int(today_partition)))
        logger.info("New Schema:".format(merged_data.printSchema()))
        logger.info(merged_data.printSchema())
        # based on day (closing day) repartition the data..
        merged_data.repartition(20).write.mode('overwrite').format('parquet').partitionBy(table_dict['partition_col_name']).save(target_path)
        logger.info('Write Final Data completed')
        
        os.system(''' aws s3 mv {0} s3://mbk-data-backup/snapshots_edits_backup/{1}/{2}/day={3}/ --recursive'''.format(source_path,table_dict['schema'],table_dict['table'],today_partition))
        #glueContext.purge_s3_path(source_path,{"retentionPeriod":0})
    except Exception as e:
        raise e
        logger.info(e)
        print('sync_snapshot failed')


def write_data(final_data, par_col):

    logger.info('Write Final Data Started')
    logger.info("Schema:".format(final_data.printSchema()))
    final_data.repartition(20).write.mode('overwrite').format('parquet').partitionBy(par_col).save(target_path)
    logger.info('Write Final Data completed')


#Converting timestamp and double data

def dedupe_data(data, pk_list, audit_col):
    logger.info('Dedupe Started')
    window = Window.partitionBy(pk_list).orderBy(col(audit_col).desc())
    data_uq = data.select("*", row_number().over(window).alias("r_num")).filter(
        col("r_num") == 1).drop("r_num")
    logger.info('Dedupe completed')
    return data_uq


def reformat_source_data(src_data):
    try:
        logger.info('Reformatting of data started')
        logger.info("Decimal cols: {}".format(table_dict['decimal_cols']))
        logger.info("Timestamp cols: {}".format(table_dict['timestamp_cols']))
        
        if not src_data.rdd.isEmpty():
            delta_day = src_data
            if bool(table_dict.get('timestamp_cols')):
                delta_day_time = delta_day.select(
                    *(col(c).cast("timestamp").alias(c) if c.lower() in table_dict['timestamp_cols'].split(',') else c for c in
                      delta_day.columns))
            else:
                delta_day_time = delta_day
            if bool(table_dict.get('decimal_cols')):
                delta_day_final = delta_day_time.select(
                    *(col(c).cast("double").alias(c) if c.lower() in table_dict['decimal_cols'].split(',') else c for c in
                      delta_day_time.columns))
            else:
                delta_day_final = delta_day_time

            return delta_day_final
            
    except Exception as e:
        logger.info(e)
        print('reformat_source_data failed')
        raise e
        

def read_raw_data():
    logger.info("Glue job started, reading raw data from location: {}".format(source_path_dyf))

    raw_data = glueContext.create_dynamic_frame.from_options( \
                connection_type="s3", \
                connection_options=source_path_dyf, \
                format="parquet" \
                ).toDF()

    if raw_data.rdd.isEmpty():
        logger.info("No data found for date {}, at location {}".format(today_partition, source_path_dyf))
        raise ("No data found for the date {}, at location {}".format(today_partition, source_path_dyf))
    return raw_data

    



        
def start_job():

    src_data=read_raw_data()
    
    logger.info('Formatting data')
    formatted_data=reformat_source_data(src_data)

    logger.info('Deduping data')

    data_unq=dedupe_data(formatted_data, pk_list, table_dict['audit_col'])

    par_col=table_dict['partition_col_name']

    logger.info('PartitionCol name is: {}'.format(par_col))

    final_data=data_unq.withColumn(par_col, lit(int(today_partition)))

    write_data(final_data, par_col)
   


start_job()


#sync_snapshot()