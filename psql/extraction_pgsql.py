import sqlalchemy
import pymysql
import pandas as pd
pymysql.install_as_MySQLdb()
from datetime import datetime,timedelta
from datetime import date
import yaml
from pyathena import connect
import os
from airflow.hooks.postgres import PostgresHook




PGPASSWORD=TcvgTb5@I7#8#g8Ti psql -U ombkpsqlread01 -h 10.10.150.24 -d merchant

host="10.10.150.24"
user_name="ombkpsqlread01"
passw="TcvgTb5@I7#8#g8Ti"
db_name="merchant"
conn_url=f"postgresql://{user_name}:{passw}@{host}/{db_name}"

engine = sqlalchemy.create_engine(conn_url)


query="SELECT * FROM mprofile_transactiontemplate WHERE 1=1 limit 10" 

mysql_count = pd.read_sql(query,engine)


def fetch_records2(ds, ds_nodash):
    # Execution date, date on which DAG was triggered..format=    ds=>2023-04-21, ds_nodash=>20230401

    # Start using these dates in query instead of hardcoded, or curr_date()
    print(f"ds: {ds}, ds_nodash:{ds_nodash}")

    request = "SELECT * FROM mobinew.member limit 10"
    pgsql_merchant_conn='pgsql_150.24_merchant'
    pgsql_hook = PostgresHook(pgsql_conn_id=pgsql_merchant_conn, schema='merchant')
    query1 = "select * from merchant.mprofile_merchantprofile  LIMIT 5"


    df1 = mysql_hook.get_pandas_df(sql=query1)

