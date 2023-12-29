__author__ = 'ketankk'

import logging

import boto3
import pandas as pd

log = logging.getLogger(__name__)

# TODO Remove, only for testing in Python shell
import sys
logging.basicConfig(level=logging.INFO, stream=sys.stdout)


# Common landing zone for database `external_data_schema`
s3_landingzone = "s3://mbk-nifi-landingzone/data/external_data_schema/{table_name}"


# Glue Job
def credgenics_load_data():
    log.info('Running Glue Job')
    glue_client = boto3.client('glue', region_name="ap-south-1")
    target_db = "external_data_schema"
    target_table = "credgenics_calling_data"
    metafile = f"s3://mbk-nifi-landingzone/replication_v2/configs/{target_db}/{target_table}.glue.csv"

    args = {'--type': '1', '--y_day': '20230401', '--d_day': '20230420', '--table_metafile': metafile}

    response = glue_client.start_job_run(
        JobName='rep_mbk_snapshot',
        Arguments=args)

    def check_status(job_name, run_id):
        while True:
            status_response = glue_client.get_job_run(JobName=job_name, RunId=run_id)
            run_status = status_response['JobRun']['JobRunState']
            print(run_status)
            time.sleep(5)
            if run_status == 'FAILED':
                print('Job Failed..:(')
                break
            if run_status == 'SUCCEEDED':
                print('Job Successful..!!')
                break
        return run_status




# Load data..
def credgenics_fetch_data(ds):
    bucket_name = "mbk-client-sftp-prod"
    table_name = "credgenics_calling_data"
    # TODO Remove
    ds='20231229'
#
    # Moving file to new location once it has been uploaded to Nifi landing zone to avoid duplication
    def move_file(s3client, exec_date, src_key):
        copy_source = {'Bucket': bucket_name, 'Key': src_key}
        target_key = f"credgenics/processed/{exec_date}/{src_key.split('/')[-1]}"
        log.info(f"Moving file: {src_key} to: {target_key}")
        s3client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=target_key)
        # TODO
        #client.delete_object(Bucket=buck_name, Key=src_key)
        log.info("File copied and deleted")
#
    # Save csv file to landingzone in parquet format, to be consumed by Glue Job credgenics_load_data() @cred
    def save_file(df):
        import uuid
        uid = str(uuid.uuid4())
        tgt_loc = s3_landingzone.format(table_name=table_name)
        tgt_loc = f"{tgt_loc}/{uid}.parquet"
        log.info(f"Saving file to location: {tgt_loc}")
        df.to_parquet(tgt_loc, index=False)
        return True
#
    # Renaming columns to remove spaces and keeping it as lower case
    def rename_columns(df):
        log.info(f"Renaming columns: {df.columns}")
        df.columns = map(str.lower, df.columns)
        df.rename(columns={'campaign name': 'campaign_name',
                           'campaign id': 'campaign_id',
                           'loan id': 'loan_id',
                           'dpd bucket': 'dpd_bucket',
                           'allocation month': 'allocation_month',
                           'call type': 'call_type',
                           'agent name': 'agent_name',
                           'agent email id': 'agent_email_id',
                           'did number': 'did_number',
                           'customer contact number': 'cust_contact_num',
                           'shoot id': 'shoot_id',
                           'call start time': 'call_start_time',
                           'customer leg ringing start time': 'cust_lr_st',
                           'customer leg ringing end time': 'cust_lr_et',
                           'agent leg ringing start time': 'agent_lr_st',
                           'agent leg ringing end time': 'agent_lr_et',
                           'total ringing time duration': 'total_ring_time_duration',
                           'customer call pickup time': 'cust_call_pickup_time',
                           'agent call pickup time': 'agent_call_pickup_time',
                           'call end time': 'call_end_time',
                           'total talk time duration': 'tot_talk_time_dur',
                           'total call duration': 'tot_call_dur',
                           'customer wait duration': 'cust_wait_dur',
                           'wrapup start time': 'wrapup_start_time',
                           'wrapup end time': 'wrapup_end_time',
                           'wrapup duration': 'wrapup_duration',
                           'call disconnected by': 'call_disconnected_by',
                           'call disconnection stage': 'call_disconnection_stage',
                           'call status': 'call_status',
                           'disposition': 'disposition',
                           'sub disposition 1': 'sub_disposition_1',
                           'sub disposition 2': 'sub_disposition_2',
                           'dialer disposition': 'dialer_disposition',
                           'call transfer from': 'call_transfer_from',
                           'call transfer to': 'call_transfer_to',
                           'call transfer type': 'call_transfer_type',
                           'call response': 'call_response',
                           'reminder date': 'reminder_date',
                           'committed amount': 'committed_amount',
                           'role': 'role',
                           'recording_link': 'recording_link'}, inplace=True)
        log.info(f"Renamed columns: {df.columns}")
        return df
#
    def get_files(s3client, prefix):
        response = s3client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        all_files = response.get('Contents')
        if all_files is None or len(all_files) is 0:
            log.error(f'No files found with prefix: {prefix}')
            raise Exception(f'No files found with prefix: {prefix}')
        log.info(f"Number of files found are: {len(all_files)}")
        for file in all_files:
            file_name = file.get('Key')
            uploaded_on = file.get('LastModified')
            log.info(f"File: {file_name}, uploaded on FTP on: {uploaded_on}")
            file_loc = f"s3://{bucket_name}/{file_name}"
            log.info(f"Processing file: {file_loc}")
            df = pd.read_csv(file_loc)
            log.info(f"Number of entries found in file: {file_loc} is {df.shape[0]}, and number of columns is: {df.shape[1]}")
            df = rename_columns(df)
            if save_file(df):
                move_file(s3client, ds, file_name)
        return True
#
    region = 'ap-south-1'
    # s3_loc = "s3://mbk-client-sftp-prod"
    prefix = "credgenics/CALL_DETAIL_REPORT_calling_call_details"
    # For testing
    prefix = "credgenics/CDR_calling_call_details"
    s3client = boto3.client('s3', region_name=region)
    get_files(s3client, prefix)


## Shoot ID	Call Start Time


credgenics_fetch_data('2023-12-21')
