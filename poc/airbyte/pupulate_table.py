__author__ = 'ketankk'

import time
import os

import mysql.connector
from mysql.connector import Error
from faker import Faker
Faker.seed(33422)
import uuid
fake = Faker()

'''
mid, orderid, txnamount, statecode, createdat, updated_at
'''
#draco_da is aliased to `mysql -u analytics -p'vsn@0pl3TYujk23(o' -h data-analytics-mysql-prod.mbkinternal.in -D mobinew -A'

db_host = 'da-poc.cythlqtrzz2a.ap-south-1.rds.amazonaws.com'
port=3308
db_name = 'airbyte_poc'
db_user = 'da_poc_user'
db_pass = 'hsj1qa#rEtyuO1P'
# da-poc.cythlqtrzz2a.ap-south-1.rds.amazonaws.com:3308	da_poc_user	hsj1qa#rEtyuO1P


try:
    conn = mysql.connector.connect(host=db_host, database = db_name, user=db_user, password=db_pass, port=port)

    if conn.is_connected():
        cursor = conn.cursor()

        try:
            cursor.execute(create_table_sql)
            print("Table created")
        except Exception as e:
            print("Error creating table", e)
        row = {}
        n = 0

        while True:
            n += 1
            row = [fake.first_name(), fake.last_name(), fake.email(), fake.postcode(), fake.city(), fake.country(), fake.date_of_birth()]

            uu = str(uuid.uuid4())
            mid = uu.split('-')[2]
            order_id = uu.split('-')[-1]
            query=f"INSERT INTO airbyte_poc.airbyte_txp (mid,  orderid, txnamount, statecode) VALUES ('{mid}', '{order_id}', '{fake.random_int()}', '{fake.random_digit()}');"
            cursor.execute(query)
            conn.commit()

            if n % 100 == 0:
                print("iteration %s" % n)
                time.sleep(0.5)
                conn.commit()
except Error as e :
    print ("error", e)
    pass
except Exception as e:
    print ("Unknown error %s", e)
finally:
    #closing database connection.
    if(conn and conn.is_connected()):
        conn.commit()
        cursor.close()
        conn.close()


########

uu = str(uuid.uuid4())
mid = uu.split('-')[2]
order_id = uu.split('-')[-1]
query=f"INSERT INTO airbyte_poc.airbyte_txp (mid,  orderid, txnamount, statecode) VALUES ('{mid}', '{order_id}', '{fake.random_int()}', '{fake.random_digit()}');"
cursor.execute(query)
conn.commit()



###############


>>> pd.read_parquet('s3://mbk-airbyte-data/POC/airbyte_poc/airbyte_txp/2023_12_06_1701848179692_0.parquet')
Traceback (most recent call last):
  File "<stdin>", line 1, in <module>
NameError: name 'pd' is not defined
>>> import pandas as pd
pd.read_parquet('s3://mbk-airbyte-data/POC/airbyte_poc/airbyte_txp/2023_12_06_1701848179692_0.parquet')

df2=pd.read_parquet('s3://mbk-airbyte-data/POC/airbyte_poc/airbyte_txp/2023_12_06_1701849246697_0.parquet')

/home/data-eng-new/.local/lib/python3.6/site-packages/OpenSSL/crypto.py:8: CryptographyDeprecationWarning: Python 3.6 is no longer supported by the Python core team. Therefore, support for it is deprecated in cryptography and will be removed in a future release.
  from cryptography import utils, x509
                         _airbyte_ab_id              _airbyte_emitted_at                                          createdat            _ab_cdc_log_file  ... id statecode      _ab_cdc_cursor  _airbyte_additional_properties
0  65e3cbfb-1fd0-4885-accb-6ea8454b1624 2023-12-06 07:36:21.306000+00:00  {'member0': 2023-12-06 07:13:00+00:00, 'member...  mysql-bin-changelog.000032  ...  1         1  170184818100000001                            None

[1 rows x 15 columns]
>>> df=pd.read_parquet('s3://mbk-airbyte-data/POC/airbyte_poc/airbyte_txp/2023_12_06_1701848179692_0.parquet')
>>> df.columns
Index(['_airbyte_ab_id', '_airbyte_emitted_at', 'createdat',
       '_ab_cdc_log_file', 'updated_at', 'orderid', 'txnamount',
       '_ab_cdc_log_pos', '_ab_cdc_updated_at', 'mid', '_ab_cdc_deleted_at',
       'id', 'statecode', '_ab_cdc_cursor', '_airbyte_additional_properties'],
      dtype='object')
>>> df['createdat']
0    {'member0': 2023-12-06 07:13:00+00:00, 'member...
Name: createdat, dtype: object
>>> df['createdat'].get('member0')
>>> df['createdat'][0].get('member0')
datetime.datetime(2023, 12, 6, 7, 13, tzinfo=<UTC>)
>>> df['createdat'][0].get('member1')
>>> df['createdat'][0].get('member1')




__author__ = 'ketankk'

import time
import os

import mysql.connector
from mysql.connector import Error
from faker import Faker
Faker.seed(33422)
import uuid
fake = Faker()

'''
mid, orderid, txnamount, statecode, createdat, updated_at
'''
#draco_da is aliased to `mysql -u analytics -p'vsn@0pl3TYujk23(o' -h data-analytics-mysql-prod.mbkinternal.in -D mobinew -A'

db_host = 'da-poc.cythlqtrzz2a.ap-south-1.rds.amazonaws.com'
port=3308
db_name = 'airbyte_poc'
db_user = 'da_poc_user'
db_pass = 'hsj1qa#rEtyuO1P'
# da-poc.cythlqtrzz2a.ap-south-1.rds.amazonaws.com:3308	da_poc_user	hsj1qa#rEtyuO1P

conn = mysql.connector.connect(host=db_host, database = db_name, user=db_user, password=db_pass, port=port)
cursor = conn.cursor()
n=0
while n<100:
    n += 1
    uu = str(uuid.uuid4())
    mid = uu.split('-')[2]
    order_id = uu.split('-')[-1]
    query=f"INSERT INTO airbyte_poc.airbyte_txp (mid,  orderid, txnamount, statecode) VALUES ('{mid}', '{order_id}', '{fake.random_int()}', '{fake.random_digit()}');"
    print(n)
    print(query)
    cursor.execute(query)

conn.commit()

alias airbyte_mysql="mysql -u da_poc_user -h da-poc.cythlqtrzz2a.ap-south-1.rds.amazonaws.com -P 3308 -p'hsj1qa#rEtyuO1P' -Dairbyte_poc"

