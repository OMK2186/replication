CREATE EXTERNAL TABLE `kk_epfo_data`(
  `memberid` string, 
  `lastupdatedat` timestamp, 
  `mobilenumber` string, 
  `overall_total_balance` bigint, 
  `establishmentname` string, 
  `epfouseremploymentdata.dateofbirth` string, 
  `epfouseremploymentdata.dateofjoining` timestamp, 
  `epfouseremploymentdata.totalemployeeshare` bigint, 
  `epfouseremploymentdata.totalemployershare` bigint, 
  `epfouseremploymentdata.totalpensionshare` bigint, 
  `epfouseremploymentdata.name` string, 
  `epfouseremploymentdata.fathername` string, 
  `employeeshare` string, 
  `employershare` string, 
  `pensionshare` string, 
  `totalbalance` bigint, 
  `dateofapproval` string, 
  `__index_level_0__` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mbk-datalake-common-prod/lending_schema/kk_epfo_data/'
