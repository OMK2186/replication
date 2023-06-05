PSQL Replication..
mprofile_payoututr

PGPASSWORD="YQGF9H371iyVO6" psql -h10.13.0.73 -U ketank -d merchant


mprofile_payoutdump--settlement date


mprofile_bulkrefund
mprofile_bulkrefundstaff
mprofile_payoutdump
mprofile_payoutdumpold
mprofile_payoututr
mprofile_submerchantprofile
mprofile_submerchanttransactiontemplate
mprofile_merchantprofile
mprofile_transactiontemplate
mprofile_wapgpayoutentry
mprofile_merchantpayoutentry
mprofile_p2pmerchantdetails
mprofile_p2pmerchantprofile


##Commands:
#show databases
\l
#To list tables
\d

# Columns of a table
\d <table-name>

select * from mprofile_payoutdump limit 10

select * from mprofile_merchantprofile limit 10



merchant=> \z mprofile_payoutdump;
                                           Access privileges
 Schema |        Name         | Type  |          Access privileges           | Column access privileges 
--------+---------------------+-------+--------------------------------------+--------------------------
 public | mprofile_payoutdump | table | ss=arwdDxt/ss                       +| 
        |                     |       | ombkpsqlread01=r/ss                 +| 
        |                     |       | merchants_dashboard_prod_rw=arwd/ss +| 
        |                     |       | merchants_dashboard__beta_rw=arwd/ss | 
(1 row)

\du
\x on

#Details about tables
\d mprofile_payoutdump 

\l list all databases.
select *
from pg_indexes
where tablename not like 'pg%';

select tablename,indexname,tablespace,indexdef  from pg_indexes where tablename = 'mprofile_payoutdump'


CREATE INDEX idx_settlement_date  ON mprofile_payoutdump(settlement_date);


jdbc:mysql://data-analytics-mysql-prod.mbkinternal.in/mobikwik_schema?zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false

jdbc:postgresql://10.13.0.73/merchant
org.postgresql.Driver


===========
${UUID()}.parquet
${Bucket}${DataPath}${Schema}/${Table}


sql_alchemy_conn = mysql+mysqlconnector://airflow_user:Uytgdr&4$kjr@localhost:3306/airflow_db



----------------------+--------------------------+---------------------------------
 id                   | integer                  | 
 transaction_id       | character varying(100)   | 
 mid                  | character varying(50)    | 
 parent_mid           | character varying(30)    | 
 is_marketplace       | boolean                  | 
 orderid              | character varying(100)   | 
 user_email           | character varying(100)   | 
 transaction_date     | timestamp with time zone | 
 transaction_amount   | numeric(13,2)            | 
 fee                  | numeric(13,2)            | 
 service_tax          | numeric(13,2)            | 
 is_gst_transaction   | boolean                  | 
 gst                  | numeric(13,2)            | 
 cgst                 | numeric(13,2)            | 
 sgst                 | numeric(13,2)            | 
 igst                 | numeric(13,2)            | 
 settlement_date      | date                     | 
 settlement_batch     | character varying(50)    | 
 settlement_type      | character varying(50)    | 
 settlement_amount    | numeric(13,2)            | 
 settlement_utr       | character varying(100)   | 
 payout_amount        | numeric(13,2)            | 
 payout_batch         | character varying(50)    | 
 payout_date          | timestamp with time zone | 
 refund_amount        | numeric(13,2)            | 
 refund_batch         | character varying(50)    | 
 refund_adjusted_date | timestamp with time zone | 
 refund_adjusted_id   | character varying(100)   | 
 
 narration            | character varying(100)   | 
 ms_query_id          | smallint                 | 
 merchant_alias       | character varying(50)    | default NULL::character varying
 lender_code          | numeric(5,0)             | default NULL::numeric

#day_start='Dag start', 
#day_end='Dag day -1'
##  select ${columns} from ${schema}.${table} where ${where} and ${auditcol}>="${day_start}" and ${auditcol}<"${day_end}";