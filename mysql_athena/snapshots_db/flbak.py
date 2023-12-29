__author__ = 'ketankk'

### This script is being used as fallback for taking snapshot
# This will run from Crontab, not dependent on airflow

from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime
from threading import Thread
from time import sleep
import uuid
import yaml
from airflow.utils.email import send_email

mysql_user = "analytics"
mysql_password = 'vsn@0pl3TYujk23(o'
mysql_host = "10.10.134.187"
mysql_db = "mobikwik_schema"

db_connection_str = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_db}'
db_connection = create_engine(db_connection_str)

connection = MySQLdb.connect(user=mysql_user, password=mysql_password, database=db, host=mysql_host)

# df = pd.read_sql('SELECT * FROM mobinew.member limit 10', con=db_connection)

### snapshotFallbackConfigFile.yaml

## Snapshot fallback using crontab


# draco_da is aliased to `mysql -u analytics -p'vsn@0pl3TYujk23(o' -h data-analytics-mysql-prod.mbkinternal.in -D mobinew -A'


# Save data on this location
s3_location = "s3://mbk-nifi-landingzone/replication_v2/data/ss_backups/mysql/"
mail_list = ["dataengg@mobikwik.com", "ketan.keshri@mobikwik.com"]


def extract_data(db_name, table_name, audit_col, day):
    print(db_name, table_name, audit_col)
    # df = pd.read_sql(f'SELECT * FROM {db_name}.{table_name} WHERE {audit_col} > CURDATE() - INTERVAL 2 DAY ', con=db_connection)
    # df = pd.read_sql(f'SELECT * FROM {db_name}.{table_name} order by {audit_col} desc limit 10', con=db_connection)
    df = pd.read_sql(f'SELECT * FROM {db_name}.{table_name} ', con=db_connection)
    print(f"Data count for {db_name}.{table_name} is {df.shape}")
    uid = str(uuid.uuid4())
    df.to_parquet(f"{s3_location}{db_name}/{table_name}/day={day}/{uid}.parquet", index=False)
    last_updated = df.sort_values(by=[audit_col], ascending=False)[audit_col][0]
    body = f"Hi, <br> Snapshot of table  {db_name}.{table_name} done for {day}."
    body = body + f"<br>Count is {df.shape[0]} !!"
    body = body + f"<br>Last {audit_col} is: {last_updated}"
    send_email(mail_list, f"<{day}>Fallback Snapshot of table  {db_name}.{table_name} ", body)


with open('/apps/cron/aws_replication/metadata/snapshotFallbackConfigFile.yaml') as f:
    configFile = yaml.safe_load(f)
    # Extract table names and fields to be processed
    tables = configFile['tables']
    day = datetime.now().strftime('%Y%m%d')
    # In this loop tasks are created for each table defined in the YAML file
    for table in tables:
        for table, fieldName in table.items():
            # print(table)
            db_name = fieldName['database_name']
            table_name = fieldName['table_name']
            audit_col = fieldName['audit_col']
            ext_data_thread = Thread(target=extract_data, args=(db_name, table_name, audit_col, day))
            ext_data_thread.start()
        # ext_data_thread.join()