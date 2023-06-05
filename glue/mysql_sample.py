import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext, SparkConf
from awsglue.context import GlueContext
from awsglue.job import Job
import time
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

connection_mysql = {
    "url": "jdbc:mysql://data-analytics-mysql-prod.mbkinternal.in:3306/byod",
    "dbtable": "employee",
    "user": "MySQLadmin",
    "password": "MYSQLadmin123",
    #"customJdbcDriverS3Path": "s3://<Bucket>/<Folder>/mysql-connector-java-8.0.19.jar",
    "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver"}


# Read DynamicFrame from Oracle
df_emp = glueContext.create_dynamic_frame.from_options(connection_type="mysql", connection_options=connection_mysql)


df_emp = ApplyMapping.apply(frame = df_emp, mappings = [("employee_id", "integer", "employee_id", "integer"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("email", "string", "email", "string"), ("phone_number", "string", "phone_number", "string"), ("hire_date", "string", "hire_date", "string"), ("job_id", "string", "job_id", "string"), ("salary", "long", "salary", "long"), ("commission_pct", "long", "commission_pct", "long"), ("manager_id", "long", "manager_id", "long"), ("department_id", "integer", "department_id", "integer")])



df_emp_dept = Join.apply(df_emp, df_dept, 'department_id', 'dept_id')
df_emp_dept = df_emp_dept.drop_fields(['department_id','dept_id'])
df_emp_dept = DropNullFields.apply(frame = df_emp_dept)

df_emp_dept.printSchema()

# Write data to Oracle
glueContext.write_from_options(frame_or_dfc=df_emp_dept, connection_type="oracle", connection_options=connection_oracle18_options_target_emp_dept)
