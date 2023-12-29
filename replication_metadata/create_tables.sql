--mysql -u de_replication -p'#7tJ22%$' -h data-analytics-mysql-prod.mbkinternal.in -D replication_metadata -A
--`mysql -u analytics -p'vsn@0pl3TYujk23(o' -h data-analytics-mysql-prod.mbkinternal.in -D mobinew -A'


USE replication_metadata;
#--Table to contain status of replication for a particular trigger
CREATE TABLE `replication_status` (
  `trigger_id` varchar(50) NOT NULL,
  `mapping_id` int NOT NULL,
  `target_db_name` varchar(20) DEFAULT NULL,
  `target_table_name` varchar(30) DEFAULT NULL,
  `stage` varchar(30) DEFAULT NULL COMMENT 'Nifi / Glue / Something else',
  `message` text DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`trigger_id`),
  KEY idx_target_db_name(target_db_name),
  KEY idx_target_table_name(target_table_name),
  KEY idx_status(status),
  KEY id_updated_at(updated_at)
);

'''
--Update version below
CREATE TABLE `entity_list` (
  `entity_id` int NOT NULL AUTO_INCREMENT,
  `entity_src_name` varchar(50) DEFAULT NULL,
  `entity_target_name` varchar(50) DEFAULT NULL,
  `entity_src_schema` varchar(50) DEFAULT NULL,
  `entity_target_schema` varchar(50) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`entity_id`),
  KEY id_updated_at(updated_at)
);
insert into entity_list(entity_src_name, entity_target_name, entity_src_schema, entity_target_schema) values('memberdetail', 'memberdetail','mobinew','mobinew')
---
---tables /  Collections
CREATE TABLE `entity_list` (
  `entity_id` int NOT NULL AUTO_INCREMENT,
  `entity_name` varchar(50) DEFAULT NULL,
  `entity_type` varchar(50) DEFAULT NULL,
  `entity_schema_type` varchar(50) DEFAULT NULL,
  `entity_schema_name` varchar(50) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`entity_id`),
  KEY id_updated_at(updated_at)
);

--entity_schema_type MYSQL, MONGO, PGSQL, ATHENA
insert into entity_list(entity_name, entity_type, entity_schema_type, entity_schema_name)
values('memberdetail','mysql_table','MYSQL', 'mobinew'), ('memberdetail','athena_table','ATHENA', 'mobinew')

insert into entity_list(entity_name, entity_type, entity_schema_type, entity_schema_name)
values('member_base','mysql_table','MYSQL', 'mobinew'), ('member_base','athena_table','ATHENA', 'mobinew')


--To be used by airflow
CREATE TABLE `entity_mapping` (
  `mapping_id` int NOT NULL AUTO_INCREMENT,
  `entity_src_id` int NOT NULL,
  `entity_target_id` int NOT NULL,
  `entity_src_name` varchar(50) DEFAULT NULL,
  `entity_target_name` varchar(50) DEFAULT NULL,
  `entity_src_schema` varchar(50) DEFAULT NULL,
  `entity_target_schema` varchar(50) DEFAULT NULL,
  `af_dag_id` varchar(100) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`mapping_id`),
  KEY idx_updated_at(updated_at),
  KEY idx_af_dag_id(af_dag_id),
  FOREIGN KEY (entity_src_id)
        REFERENCES entity_list(entity_id),
  FOREIGN KEY (entity_target_id)
        REFERENCES entity_list(entity_id)
);

# ALTER TABLE entity_mapping ADD af_dag_id VARCHAR(100) AFTER entity_target_schema;


insert into entity_mapping(entity_src_id, entity_target_id, entity_src_name, entity_target_name, entity_src_schema, entity_target_schema, af_dag_id)
values(1,2,'memberdetail','memberdetail','mobinew','mobinew', 'replicationV2_mysql_mobinew')
insert into entity_mapping(entity_src_id, entity_target_id, entity_src_name, entity_target_name, entity_src_schema, entity_target_schema, af_dag_id)
values(3,4,'member_base','member_base','mobinew','mobinew', 'replicationV2_mysql_mobinew')

--NIFI Metadata


create table nifi_metadata(
  `id` int NOT NULL AUTO_INCREMENT,
  `mapping_id` int,
  `nifi_conn_id` varchar(50) DEFAULT NULL,
  `auto_incr_col` varchar(50) DEFAULT NULL,
  `audit_col_name` varchar(50) DEFAULT NULL,
  `max_audit_val` varchar(50) DEFAULT NULL,
  `default_new_audit_val` varchar(50) DEFAULT NULL,
  `nifi_target_bucket` varchar(50) DEFAULT NULL,
  `nifi_target_loc_prefix` varchar(100) DEFAULT '',
  `batch_size` int default 100000,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  FOREIGN KEY (mapping_id)
        REFERENCES entity_mapping(mapping_id)
);


insert into nifi_metadata(`mapping_id`,`nifi_conn_id`,`auto_incr_col`,`audit_col_name`,`max_audit_val`,`nifi_target_bucket`,`nifi_target_loc_prefix`, `default_new_audit_val`)
values(1, 'mysql_an_250', null, 'updatedat', '2001-01-01', 'mbk-nifi-landingzone', 'replication_v2/data'),
(2, 'mysql_an_250', null, 'updatedAt', '2001-01-01', 'mbk-nifi-landingzone', 'replication_v2/data')


create table glue_metadata(
  `id` int NOT NULL AUTO_INCREMENT,
  `mapping_id` int,
  `src_db_name` varchar(50) DEFAULT NULL,
  `src_table_name` varchar(50) DEFAULT NULL,
  `target_db_name` varchar(50) DEFAULT NULL,
  `target_table_name` varchar(50) DEFAULT NULL,
  `load_type` varchar(50) DEFAULT NULL,
  `primary_keys` varchar(50) DEFAULT NULL,
  `audit_col_name` varchar(50) DEFAULT NULL,
  `partition_col` varchar(50) DEFAULT NULL,
  --`partition_src_col` varchar(50) DEFAULT NULL,
  `partition_col_src` varchar(50) DEFAULT NULL,
  `glue_target_bucket` varchar(50) DEFAULT NULL,
  `glue_target_loc_prefix` varchar(50) DEFAULT '',
  `decimal_cols` text DEFAULT NULL,
  `timestamp_cols` text DEFAULT NULL,
  `date_cols` text DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  FOREIGN KEY (mapping_id)
        REFERENCES entity_mapping(mapping_id)
);
insert into glue_metadata
(`mapping_id`, `src_db_name`, `src_table_name`, `load_type`, `target_db_name`, `target_table_name`, `primary_keys`,
`audit_col_name`, partition_col`, `partition_col_src`, `glue_target_bucket`, `decimal_cols`, `timestamp_cols`,  `date_cols`)
values(1,'mobinew','memberdetail','CDC','mobinew', 'memberdetail', 'memberuid', 'updatedat', 'day', 'createdat', 'mbk-datalake-common-prod',
null, 'createdat,birthday,rechargedate,reportedAt',null ),
(2,'mobinew','member_base','CDC','mobinew', 'member_base', 'id', 'updatedat', 'day', 'createdat', 'mbk-datalake-common-prod',
null, 'createdat,updatedat,rechargedate,reportedAt',null )


nifi_query = f"select nifi_conn_id, auto_incr_col, audit_col_name, max_audit_val, nifi_target_bucket, nifi_target_loc_prefix where mapping_id={mapping_id}"


--#############

INSERT INTO mobikwik_schema.replication_status(trigger_id, mapping_id, target_db_name, target_table_name, stage, message, status)
VALUES ('959cb132b17a_1', 1, 'mobinew', 'memberdetail', 'NIFI', 'Data Extracted', 'RUNNING')
  ON DUPLICATE KEY UPDATE trigger_id = '959cb132b17a_1';

INSERT INTO mobikwik_schema.replication_status(trigger_id, mapping_id, target_db_name, target_table_name, stage, message, status)
VALUES ('959cb132b17a_1', 1, 'mobinew', 'memberdetail', 'GLUE', 'Data replicated', 'SUCCESS')
  ON DUPLICATE KEY UPDATE
  stage='GLUE',
  message='Data replicated',
  status='SUCCESS'


INSERT INTO replication_status(trigger_id, mapping_id, target_db_name, target_table_name, stage, message, status)
VALUES (${trigger_id}, ${mapping_id}, ${target_db_name}, ${target_table_name}, 'NIFI-'${stage}, ${msg}, ${status})
  ON DUPLICATE KEY UPDATE
  stage='NIFI-'${stage},
  message=${msg},
  status=${status}