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
from datetime import datetime,timedelta
from pyspark.sql.functions import lit


# table_metafile 
#s3://mbk-nifi-landingzone/Glue/mobinew_memberbalance.txt

# mobinew memberbalance

glueContext = GlueContext(SparkContext.getOrCreate())


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

source_path = "s3://" + table_dict['SourceBucket'] + "/" + table_dict['RawPath'] + "/" + table_dict['Schema'] + "/" + \
              table_dict['Table'] + "/"
target_path = "s3://" + table_dict['TargetBucket'] + "/" + table_dict['Schema'] + "/" + \
              table_dict['Table'] + "/"

source_path= "s3://mbk-data-backup/memberbalance_edits_backup/memberbalance/day=20230122/"      

source_path_dyf = {"paths": [source_path]}
target_path_dyf = {"paths": [target_path]}
s3 = boto3.resource('s3')
query_string = 'MSCK repair table ' + table_dict['Schema'] + '.' + table_dict['Table']
pk_list = [pk_col for pk_col in table_dict['PrimaryKey'].split(',')]

today_partition = "20230122"
yesterday_partition = "20230120"


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




def read_source_data(dedupe):
    try:
        logger.info('New data read started')
        src_data = glueContext.create_dynamic_frame.from_options( \
            connection_type="s3", \
            connection_options=source_path_dyf, \
            format="parquet" \
            ).toDF()

        
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


def sync_cdc():
    try:
        dedupe = True
        delta_day_double = read_source_data(dedupe)
        delta_day_double=delta_day_double.toDF(*[c.lower() for c in delta_day_double.columns])

        if delta_day_double is None:
            return None
        pushdownpredicate = "(day=='" + str(yesterday_partition) + "')"
        logger.info('Read Existing Data - New Day partition '+pushdownpredicate)
        final_data = glueContext.create_dynamic_frame.from_catalog(database=table_dict['Schema'],
                                                                   table_name=table_dict['Table'],
                                                                   push_down_predicate=pushdownpredicate).toDF()
        logger.info('Read Existing Data Complete')
        logger.info('Pushdownpredicate is {0}'.format(pushdownpredicate))
        if not final_data.rdd.isEmpty():
            logger.info('Data Merge Started')
            old_data = final_data.join(delta_day_double, pk_list, "left_anti")
            merged_data = customUnion(old_data,delta_day_double)
            logger.info('Data Merge Completed')
        else:
            merged_data = delta_day_double
        logger.info('Write Final Data Started')
        #logger.info(merged_data.columns)
        merged_data=merged_data.withColumn('day', lit(int(today_partition)))
        #logger.info(merged_data.printSchema())
        merged_data.repartition(20).write.mode('overwrite').format('parquet').partitionBy(table_dict['PartitionCol']).save(target_path)
        logger.info('Write Final Data completed')
    except Exception as e:
        raise e
        logger.info(e)
        print('sync_cdc failed')


sync_cdc()
    

${now():format("yyyyMMdd")}
