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


def get_primary_key(schema, table):
    query = '''SELECT column_name
                FROM information_schema.KEY_COLUMN_USAGE
                WHERE table_schema='{0}' and TABLE_NAME = '{1}'
                AND CONSTRAINT_NAME = 'PRIMARY' '''.format(schema,table)
    primary_key = pd.read_sql(query,engine)
    primary_key = primary_key['COLUMN_NAME'].values.tolist()
    primary_key = ','.join(primary_key)
    print(primary_key)
    return primary_key



SELECT column_name FROM information_schema.KEY_COLUMN_USAGE   WHERE table_schema='mobinew' and TABLE_NAME = 'member'



SELECT  * FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = 'mobinew' and TABLE_NAME = 'member'

mysql> SELECT  * FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = 'mobinew' and TABLE_NAME = 'member' and COLUMN_NAME='CreatedAt';


ALTER TABLE {table_schema}.{table_name} ADD INDEX idx_an_lower({col_name})(col_name)


[data-eng-new@data-analytics-common-nifi-prod-01 ~]$ aws s3api delete-object --bucket mbk-datalake-common-prod --key mobinew/memberbalance/day=20230629/part-00019-4e0c0e0f-2aa5-4ffb-8ba6-a65d23875298.c000.snappy.parquet --version-id pNYjC9uE2ql6xFEgYFNJiCvUdiwGVFcV
recoverfiles=$(aws s3api list-object-versions --bucket mbk-datalake-common-prod  --prefix mobinew/memberbalance/ --query "DeleteMarkers[?IsLatest && starts_with(LastModified,'2023-07-01')].{Key:Key,VersionId:VersionId}")

for row in  $(echo "${recoverfiles}" | jq -c '.[]'); do
    key=$(echo "${row}" | jq -r '.Key'  )
    versionId=$(echo "${row}" | jq -r '.VersionId'  )
    echo aws s3api delete-object --bucket mbk-datalake-common-prod --key $key --version-id $versionId
done
