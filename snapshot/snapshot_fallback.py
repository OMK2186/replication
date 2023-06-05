from sqlalchemy import create_engine
import pandas as pd

mysql_user="analytics"
mysql_password='vsn@0pl3TYujk23(o'
mysql_host="data-analytics-mysql-prod.mbkinternal.in"
mysql_db="mobikwik_schema"


db_connection_str = f'mysql+pymysql://{mysql_user}:mysql_password@mysql_host/mysql_db'
db_connection = create_engine(db_connection_str)

df = pd.read_sql('SELECT * FROM mobinew.member limit 10', con=db_connection)



#draco_da is aliased to `mysql -u analytics -p'vsn@0pl3TYujk23(o' -h data-analytics-mysql-prod.mbkinternal.in -D mobinew -A'
