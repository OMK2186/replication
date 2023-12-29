import sqlalchemy
import pymysql
import pandas as pd
pymysql.install_as_MySQLdb()
from datetime import datetime,timedelta
from datetime import date
import yaml
from pyathena import connect
import os



PGPASSWORD=TcvgTb5@I7#8#g8Ti psql -U ombkpsqlread01 -h 10.10.150.24 -d merchant

host="10.10.150.24"
user_name="ombkpsqlread01"
passw="TcvgTb5@I7#8#g8Ti"
db_name="merchant"
conn_url=f"postgresql://{user_name}:{passw}@{host}/{db_name}"

engine = sqlalchemy.create_engine(conn_url)


query="SELECT * FROM mprofile_transactiontemplate WHERE 1=1 limit 10" 

mysql_count = pd.read_sql(query,engine)




import sqlalchemy
import pymysql
import pandas as pd
pymysql.install_as_MySQLdb()
from datetime import datetime,timedelta
from datetime import date
import yaml
from pyathena import connect
import os
engine = sqlalchemy.create_engine('mysql://analytics:vsn@0pl3TYujk23(o@data-analytics-mysql-prod.mbkinternal.in')
mysql_query="select * from mobinew.tx_request where createdat>'2023-05-30'"
mysql_count = pd.read_sql(mysql_query,engine)