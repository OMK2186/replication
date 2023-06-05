CREATE EXTERNAL TABLE `mg_mobikwik.onboarding_request`(
  `obj_id` string, 
  `member_id` string, 
  `legal_name` string, 
  `brand_name` string, 
  `qr_code` string, 
  `self_onboarding` string,
  `permanent_address` string,
  `city` string,
  `user_kyc` string,
  `category` string,
  `retailagent_id` string,
  `retailagent_memberid` string,
  `ur_lat` double,
  `ur_long` double,
  `created_at` timestamp, 
  `updated_at` timestamp
  )
PARTITIONED BY ( 
  `day` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mbk-datalake-common-prod/mg_mobinew/onboarding_request'
