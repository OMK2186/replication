CREATE EXTERNAL TABLE pg_merchant.mprofile_payoutdump (id integer,
transaction_id string,
mid string,
parent_mid string,
is_marketplace boolean,
orderid string,
user_email string,
transaction_date timestamp,
transaction_amount double,
fee double,
service_tax double,
is_gst_transaction boolean,
gst double,
cgst double,
sgst double,
igst double,
settlement_date date,
settlement_batch string,
settlement_type string,
settlement_amount double,
settlement_utr string,
payout_amount double,
payout_batch string,
payout_date timestamp,
refund_amount double,
refund_batch string,
refund_adjusted_date timestamp,
refund_adjusted_id string,
narration string,
ms_query_id smallint,
merchant_alias string,
lender_code int)
PARTITIONED BY(day integer)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://mbk-datalake-common-prod/pg_merchant/mprofile_payoutdump/'




