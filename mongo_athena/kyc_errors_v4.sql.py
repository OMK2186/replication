"obj_id":"$_id",
    "code":"$code",
    "description":"$description",
    "doc_request":"$docRequest",
    "data_request":"$dataRequest",
    "category":"$category",
    "category_label":"$categoryLabel",
    "enabled":"$enabled",
    "priority":"$priority"}}
    


    CREATE EXTERNAL TABLE `kyc_errors_v4`(
  `obj_id` string, 
  `code` string, 
  `description` string, 
  `doc_request` string, 
  `data_request` string, 
  `category` string, 
  `category_label` string, 
  `enabled` string, 
  `priority` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mbk-datalake-common-prod/mg_mobinew/kyc_errors_v4'