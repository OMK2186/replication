logger = glueContext.get_logger()


args = getResolvedOptions(sys.argv, ['JOB_NAME', glue_params])
glue_params = args["glue_params"]

logger.info(f"Glue Parameters passed are: {glue_params} ")



landing_bucket=glue_params["landing_bucket"] #: "mbk-nifi-landingzone",
data_path=glue_params["data_path"] #: "data",
target_bucket=glue_params["target_bucket"] #: "mbk-datalake-common-prod",
target_db=glue_params["target_db"]: "pg_merchant",
target_table=glue_params["target_table"] #: "mprofile_merchantprofile",
pk_list=glue_params["pk_list"]: ["id"],
load_type=glue_params["load_type"]: "FULL",
audit_col=glue_params["audit_col"]: null,
partition_col=glue_params["partition_col"] null




landing_bucket: 'mbk-nifi-landingzone' # Add this to particular table if you want to use diff landing_bucket for that table
data_path: 'data' # In this table extracted data will be stored. Add this to particular table if you want to use diff data_path for that table.
max_delta: 'maxdelta' #Location to store max_delta till which data has been extracted, This shoul be updated in metadata file
columns: '*' #column list in table if only particular columns needs to be extracted eg. columns:'memberid, txnid, createdat .. '
where: '1=1' #TODO check if this is necessary

#Config to be used for Glue Job
#glue_param_path:  's3://mbk-nifi-landingzone/Glue/' # not needed, will pass as a parameter directly
glue_job_name: '' #Common Glue job, If other job needs to be used, add in table params  
target_bucket: 'mbk-datalake-common-prod'


        load_type: 'CDC' 
        audit_col : null
        target_db_name: 'pg_merchant'
        target_table_name: 'mprofile_merchantprofile'
        primary_keys: 'id' #Comma separated values
        partition_col: null
        partition_src_col: null