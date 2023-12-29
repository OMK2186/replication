create table external_data_schema.credgenics_calling_data(campaign_name string,
campaign_id string,
loan_id string,
dpd_bucket string,
allocation_month string,
call_type string,
agent_name string,
agent_email_id string,
did_number string,
cust_contact_num string,
shoot_id string,
call_start_time timestamp,
cust_lr_st timestamp,
cust_lr_et timestamp,
agent_lr_st timestamp,
agent_lr_et timestamp,
total_ring_time_duration string,
cust_call_pickup_time timestamp,
agent_call_pickup_time timestamp,
call_end_time timestamp,
tot_talk_time_dur string,
tot_call_dur string,
cust_wait_dur string,
wrapup_start_time timestamp,
wrapup_end_time timestamp,
wrapup_duration string,
call_disconnected_by string,
call_disconnection_stage string,
call_status string,
disposition string,
sub_disposition_1 string,
sub_disposition_2 string,
dialer_disposition string,
call_transfer_from string,
call_transfer_to string,
call_transfer_type string,
call_response string,
reminder_date string,
committed_amount string,
role string,
recording_link string,
day string)
PARTITIONED BY (`day`)
LOCATION
  's3://mbk-datalake-common-prod/external_data_schema/credgenics_calling_data'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='snappy',)



create external table external_data_schema.credgenics_calling_data(campaign_name string,
campaign_id string,
loan_id string,
dpd_bucket string,
allocation_month string,
call_type string,
agent_name string,
agent_email_id string,
did_number string,
cust_contact_num string,
shoot_id string,
call_start_time timestamp,
cust_lr_st timestamp,
cust_lr_et timestamp,
agent_lr_st timestamp,
agent_lr_et timestamp,
total_ring_time_duration string,
cust_call_pickup_time timestamp,
agent_call_pickup_time timestamp,
call_end_time timestamp,
tot_talk_time_dur string,
tot_call_dur string,
cust_wait_dur string,
wrapup_start_time timestamp,
wrapup_end_time timestamp,
wrapup_duration string,
call_disconnected_by string,
call_disconnection_stage string,
call_status string,
disposition string,
sub_disposition_1 string,
sub_disposition_2 string,
dialer_disposition string,
call_transfer_from string,
call_transfer_to string,
call_transfer_type string,
call_response string,
reminder_date string,
committed_amount string,
role string,
recording_link string)

PARTITIONED BY (
  `day` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://mbk-datalake-common-prod/external_data_schema/credgenics_calling_data'

