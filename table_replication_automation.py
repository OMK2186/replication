import sqlalchemy
import pymysql
import pandas as pd
pymysql.install_as_MySQLdb()
from datetime import datetime,timedelta
from datetime import date
import yaml
from pyathena import connect
engine = sqlalchemy.create_engine('mysql://analytics:vsn@0pl3TYujk23(o@data-analytics-mysql-prod.mbkinternal.in')
import re
import os
import json


base_location = 's3://mbk-datalake-common-prod/'
RawPath = 'data'
FinalPath = ''
SourceBucket = 'mbk-nifi-landingzone'
TargetBucket = 'mbk-datalake-common-prod'
Bucket = 'mbk-nifi-landingzone'
PartitionCol = 'day'
LoadType= 'Full'
DataPath = '/data/'
MaxDelta = '/maxdelta/'
columns = '*'
where = '1=1'




def get_decimal_col(one_line_ddl,schema,table):
    output_ddl = re.sub('varchar.*\n', 'string,\n', one_line_ddl)

    output_ddl = re.sub('char.*\n', 'string,\n', output_ddl)
    output_ddl = re.sub('text.*\n', 'string,\n', output_ddl)
    output_ddl = re.sub(' int.*\n', ' int,\n', output_ddl)
    output_ddl = re.sub('bigint.*\n', 'bigint,\n', output_ddl)
    output_ddl = re.sub('datetime.*\n', 'timestamp,\n', output_ddl)
    output_ddl = re.sub('timestamp.*\n', 'timestamp,\n', output_ddl)
    output_ddl = re.sub('double.*\n', 'double,\n', output_ddl)
    output_ddl = re.sub('tinyint.*\n', 'int,\n', output_ddl)
    output_ddl = re.sub('smallint.*\n', 'int,\n', output_ddl)
    output_ddl = re.sub('PRIMARY.*|KEY.*|ENGINE.*', '', output_ddl)
    output_ddl = re.sub("`",'',output_ddl)
    #output_ddl = re.sub('date.*\n', 'timestamp,\n', output_ddl)
    last_char_index = output_ddl.rfind(",")
    output_ddl = output_ddl[:last_char_index] + ")" + "\n partitioned by (day string) \n stored as parquet\n " + 'LOCATION'
    output_ddl = re.sub('CREATE TABLE ', 'CREATE EXTERNAL TABLE {0}.'.format(schema), output_ddl)
    output_ddl = re.sub('LOCATION', r'LOCATION "{0}{1}/{2}"'.format(base_location,schema,table), output_ddl)
    result = re.findall(r"\w+ decimal",output_ddl)
    result = ','.join(result).replace(' decimal','')
    print('Decimal Cols - ',result)
    return result

def get_create_table(one_line_ddl,schema,table):
    output_ddl = re.sub('varchar.*\n', 'string,\n', one_line_ddl)

    output_ddl = re.sub('char.*\n', 'string,\n', output_ddl)
    output_ddl = re.sub('text.*\n', 'string,\n', output_ddl)
    output_ddl = re.sub(' int.*\n', ' int,\n', output_ddl)
    output_ddl = re.sub('bigint.*\n', 'bigint,\n', output_ddl)
    output_ddl = re.sub('datetime.*\n', 'timestamp,\n', output_ddl)
    output_ddl = re.sub('double.*\n', 'double,\n', output_ddl)
    output_ddl = re.sub('timestamp.*\n', 'timestamp,\n', output_ddl)
    output_ddl = re.sub('tinyint.*\n', 'int,\n', output_ddl)
    output_ddl = re.sub('smallint.*\n', 'int,\n', output_ddl)
    output_ddl = re.sub('PRIMARY.*|KEY.*|ENGINE.*', '', output_ddl)
    output_ddl = re.sub("decimal(\(.*?\)).*\n",'double,\n',output_ddl)
    output_ddl = re.sub("`",'',output_ddl)
    #output_ddl = re.sub('date.*\n', 'timestamp,\n', output_ddl)
    last_char_index = output_ddl.rfind(",")
    output_ddl = output_ddl[:last_char_index] + ")" + "\n partitioned by (day string) \n stored as parquet\n " + 'LOCATION'
    output_ddl = re.sub('CREATE TABLE ', 'CREATE EXTERNAL TABLE {0}.'.format(schema), output_ddl)
    output_ddl = re.sub('LOCATION', r'LOCATION "{0}{1}/{2}"'.format(base_location,schema,table), output_ddl)
    print(output_ddl)
    return output_ddl




#get timestamp cols
def get_timestamp_col(output_ddl,schema,table):
    result = re.findall(r"\w+ timestamp",output_ddl)
    result = ','.join(result).replace(' timestamp','')
    print('Timestamp Cols - ',result)
    return result



def get_primary_key(schema,table):
    query = '''SELECT column_name
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE table_schema='{0}' and TABLE_NAME = '{1}'
                AND CONSTRAINT_NAME = 'PRIMARY' '''.format(schema,table)
    primary_key = pd.read_sql(query,engine)
    primary_key = primary_key['column_name'].values.tolist()
    primary_key = ','.join(primary_key)
    print(primary_key)
    return primary_key



def get_ddl(schema,table):
    create_table_statement = pd.read_sql('show create table {0}.{1};'.format(schema,table),engine)
    one_line_ddl = create_table_statement['Create Table'][0]
    decimal_cols = get_decimal_col(one_line_ddl,schema,table)
    output_ddl = get_create_table(one_line_ddl,schema,table)
    timestamp_cols = get_timestamp_col(output_ddl,schema,table)
    primary_key = get_primary_key(schema,table)
    return decimal_cols,output_ddl,timestamp_cols,primary_key




def create_nifi_params(Schema,Table,LoadType,AuditCol,Bucket,DataPath,MaxDelta,columns,where):
    keys = ['Schema','Table','LoadType','AuditCol','Bucket','DataPath','MaxDelta','columns','where']
    values = [Schema,Table,LoadType,AuditCol,Bucket,DataPath,MaxDelta,columns,where]
    nifi_params= pd.DataFrame(list(zip(keys, values)),columns =['Keys', 'Values'])
    nifi_params.to_csv('/apps/nifi/table_metadata/{0}_{1}.txt'.format(Schema,Table),index=False,header=False)




def create_glue_metadata(Schema,Table,RawPath,FinalPath,SourceBucket,TargetBucket,SyncType,AuditCol,PartitionCol,PartitionSrcCol,PrimaryKey,TimestampCol,DecimalCol):
    keys = ['Schema','Table','RawPath','FinalPath','SourceBucket','TargetBucket','SyncType','AuditCol','PartitionCol','PartitionSrcCol','PrimaryKey','TimestampCol','DecimalCol']
    values = Schema,Table,RawPath,FinalPath,SourceBucket,TargetBucket,SyncType,AuditCol,PartitionCol,PartitionSrcCol,PrimaryKey,TimestampCol,DecimalCol
    glue_params = pd.DataFrame(list(zip(keys,values)),columns=['Keys','Values'])
    glue_params.to_csv('s3://mbk-nifi-landingzone/Glue/{0}_{1}.txt'.format(Schema,Table),index=False,header=False,sep='|')


def upload_lastupdated_file(schema,table):
    days = timedelta(days=10)
    lastmodified_date = datetime.now() - days
    lastmodified_date = str(lastmodified_date.strftime('%Y-%m-%d 00:00:00'))
    with open('/apps/nifi/lastupdated/adhoc.txt', 'w') as fp:
        lastmodified_dict = {"lastModified":lastmodified_date}
        json.dump(lastmodified_dict, fp)
    os.system('aws s3 cp {0} s3://mbk-nifi-landingzone/maxdelta/{1}/{2}/{2}.txt'.format('/apps/nifi/lastupdated/adhoc.txt',schema,table))


def start_extraction(schema,table):
    os.system('sh /apps/nifi/trigger_nifi_table.sh {0}_{1}.txt'.format(schema,table))
    print('extraction_completed')

def edit_nifi_params(Schema,Table,LoadType,AuditCol,Bucket,DataPath,MaxDelta,columns,where):
    LoadType='Cdc'
    keys = ['Schema','Table','LoadType','AuditCol','Bucket','DataPath','MaxDelta','columns','where']
    values = [Schema,Table,LoadType,AuditCol,Bucket,DataPath,MaxDelta,c/apps/nifi/success_fail/succolumns,where]
    nifi_params= pd.DataFrame(list(zip(keys, values)),columns =['Keys', 'Values'])
    nifi_params.to_csv('/apps/nifi/table_metadata/{0}_{1}.txt'.format(Schema,Table),index=False,header=False)

def set_daily_rep(schema,table,PartitionSrcCol):
    if(schema.lower()=='lending'):
        file_name = 'lendingConfigFile.yaml'
    elif(schema.lower()=='recharge'):
        file_name = 'recharge_db_replication.yaml'
    elif(schema.lower()=='mobinew'):
        file_name = 'mobinew_replication.yaml'
    elif(schema.lower()=='mobikwikupdated'):
        file_name = 'mobikwikupdated_replication.yaml'
    elif(schema.lower()=='altce'):
        file_name = 'altce_replication.yaml'
    elif(schema.lower()=='paylater'):
        file_name = 'paylaterConfigFile.yaml'
    elif(schema.lower()=='merchants_core_db'):
        file_name = 'merchants_core_db.yaml'
    else:
        return
    new_dict = {}
    new_dict['{0}_{1}'.format(schema,table)] = {'file': '{0}_{1}.txt'.format(schema,table),'database_name': schema, 'table_name': table, 'validation_report_col': PartitionSrcCol}
    with open(r'/apps/cron/aws_replication/metadata/{0}'.format(file_name), 'r') as yamlfile:
        cur_yaml = yaml.safe_load(yamlfile)  # Note the safe_load
        cur_yaml['tables'].append(new_dict)

    if cur_yaml:
        with open(r'/apps/cron/aws_replication/metadata/{0}'.format(file_name),'w') as yamlfile:
            yaml.safe_dump(cur_yaml, yamlfile) # Also note the safe_dump

    return

def print_glue_commnd(schema,table):
    query='sh /apps/nifi/glue_job_run.sh s3://mbk-nifi-landingzone/Glue/{0}_{1}.txt {0} {1}'.format(schema,table)
    print(query)


def gen_metadata():
    schema = input('Enter Schema Name')
    table = input('Enter Table Name')
    decimal_cols,output_ddl,timestamp_cols,primary_key = get_ddl(schema,table)
    print(timestamp_cols)
    print(decimal_cols)
    SyncType = input('Enter Sync type for glue metadata-> Options -> Full,Cdc,Insert ---->')
    if(SyncType.lower()!='full'):
        AuditCol = input('Enter AuditCol for dedupe-> Options -> {0} ---->'.format(timestamp_cols))
        PartitionSrcCol = input('Enter PartitionSrcCol -> Options -> {0} ---->'.format(timestamp_cols))
    else:
        AuditCol=''
        PartitionSrcCol=''
        #PartitionCol=''
    create_nifi_params(schema,table,LoadType,AuditCol,Bucket,DataPath,MaxDelta,columns,where)
    create_glue_metadata(schema,table,RawPath,FinalPath,SourceBucket,TargetBucket,SyncType,AuditCol,PartitionCol,PartitionSrcCol,primary_key,timestamp_cols,decimal_cols)
    print_glue_commnd(schema,table)
    if(SyncType.lower()!='full'):
        upload_lastupdated_file(schema,table)
    start_extraction(schema,table)
    if (SyncType.lower() != 'full'):
        edit_nifi_params(schema, table, LoadType, AuditCol, Bucket, DataPath, MaxDelta, columns, where)
    #set_daily_rep(schema, table, PartitionSrcCol)



gen_metadata()


