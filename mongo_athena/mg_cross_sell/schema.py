schema = StructType([StructField("obj_id",StringType(),True),
 StructField("member_id", StringType(),True),
 StructField("insurance_category", StringType(),True),
 StructField("insurance_sell_platform", StringType(),True),
 StructField("insurer", StringType(),True),
 StructField("amount_paid", DoubleType(),True),
 StructField("cross_sell",BooleanType(),True),
 StructField("booked_at", TimestampType(),True),
 StructField("debited_at",TimestampType(),True),
 StructField("created_at",TimestampType(),True),
 StructField("last_modified",TimestampType(),True)])





db_name="mg_cross-sell"
table_name="policy"
p_key="obj_id"
audit_col="last_modified"
insert_type="upsert"
part_col="day"

additional_options = {
    "hoodie.table.name": table_name,
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.operation": insert_type,
    "hoodie.datasource.write.recordkey.field": p_key,
    "hoodie.datasource.write.precombine.field": audit_col,
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.parquet.compression.codec": "snappy",
    "hoodie.datasource.hive_sync.enable": "True",
    "hoodie.datasource.hive_sync.database": db_name,
    "hoodie.datasource.hive_sync.table": table_name,
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
    "hoodie.datasource.write.partitionpath.field": part_col
}
target_path=f's3://mbk-datalake-common-prod/{db_name}/{table_name}/'

df2.write.format("org.apache.hudi").options(**additional_options).mode("append").save(target_path)


#src_data.toDF().write.format("parquet").save(target_path)
print('done')