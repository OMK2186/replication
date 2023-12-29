__author__ = 'ketankk'


import pysftp
host = "mbkext-sftp.mobikwik.com"
username = "credgenics"
password = "Credgenics@mbk23"



import re
from datetime import datetime
date = datetime.now().date()
dd=date.day
MM=date.month
YYYY=date.year

file_name_pattern = f"CALL_DETAIL_REPORT_calling_call_details_{YYYY}-{MM}-{dd}-*.csv"


p = re.compile('^.+-.+-.+$')
file_name_pattern = f"CALL_DETAIL_REPORT_calling_call_details_{YYYY}-{MM}-{dd}-[0-9]{{2}}:[0-9]{{2}}_(AM|PM).csv"
# CALL_DETAIL_REPORT_calling_call_details_2023-12-23-08:33_AM.csv
pattern = re.compile(file_name_pattern)



import pandas as pd
with pysftp.Connection(host, username=username, password=password) as sftp:
    all_files = sftp.listdir()
    print(all_files)
    todays_files = [file for file in all_files if pattern.match(file)]
    print(todays_files)

    file = sftp.open('CALL_DETAIL_REPORT_calling_call_details_2023-12-27-08:33_AM.csv')
    df = pd.read_csv(file)


######

#result = s3client.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
#################
import  boto3
region='ap-south-1'
s3_loc="s3://mbk-client-sftp-prod"
bucket_name="mbk-client-sftp-prod"

prefix="credgenics/CALL_DETAIL_REPORT_calling_call_details"
#For testing
prefix="credgenics/CDR_calling_call_details"


s3client = boto3.client('s3', region_name=region)
response = s3client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

content=response.get('Contents')
if content is None or len(content) is 0:
    print(f'No files found with prefix: {prefix}')
    #TODO break this

all_files=content
import re
from datetime import datetime

date = datetime.now().date()
day='2023-12-21'
#TODO for testing
date=datetime.strptime(day,'%Y-%m-%d')
dd=date.day
mm=date.month
YYYY=date.year

file_name_pattern = f"CALL_DETAIL_REPORT_calling_call_details_{YYYY}-{mm}-{dd}-[0-9]{{2}}:[0-9]{{2}}_(AM|PM).csv"
# For testing
file_name_pattern = f"credgenics/CDR_calling_call_details_{YYYY}-{mm}-{dd}-[0-9]{{2}}:[0-9]{{2}}_(AM|PM).csv"
# CALL_DETAIL_REPORT_calling_call_details_2023-12-23-08:33_AM.csv
pattern = re.compile(file_name_pattern)

## Shoot ID	Call Start Time
import pandas as pd
for file in all_files:
    print(file)
    file_name = file.get('Key')
    if pattern.match(file_name):
        print(f'File found for date: {day}, file name: {file_name}')
        file_loc=f"{s3_loc}/{file_name}"
        print(file_loc)
        df=pd.read_csv(file_loc)
        print(df.shape)

todays_files = [file for file in all_files if pattern.match(file.get('Key'))]
    print(file)

for file in todays_files:

file_name = "CDR_calling_call_details_2023-12-21-01:01_PM.csv"
## file = sftp.open('CALL_DETAIL_REPORT_calling_call_details_2023-12-27-08:33_AM.csv')
## file = sftp.open(file_name)
## df = pd.read_csv(file)

df.columns = map(str.lower, df.columns)
df.rename(columns={'campaign name':'campaign_name',
'campaign id':'campaign_id',
'loan id':'loan_id',
'dpd bucket':'dpd_bucket',
'allocation month':'allocation_month',
'call type':'call_type',
'agent name':'agent_name',
'agent email id':'agent_email_id',
'did number':'did_number',
'customer contact number':'cust_contact_num',
'shoot id':'shoot_id',
'call start time':'call_start_time',
'customer leg ringing start time':'cust_lr_st',
'customer leg ringing end time':'cust_lr_et',
'agent leg ringing start time':'agent_lr_st',
'agent leg ringing end time':'agent_lr_et',
'total ringing time duration':'total_ring_time_duration',
'customer call pickup time':'cust_call_pickup_time',
'agent call pickup time':'agent_call_pickup_time',
'call end time':'call_end_time',
'total talk time duration':'tot_talk_time_dur',
'total call duration':'tot_call_dur',
'customer wait duration':'cust_wait_dur',
'wrapup start time':'wrapup_start_time',
'wrapup end time':'wrapup_end_time',
'wrapup duration':'wrapup_duration',
'call disconnected by':'call_disconnected_by',
'call disconnection stage':'call_disconnection_stage',
'call status':'call_status',
'disposition':'disposition',
'sub disposition 1':'sub_disposition_1',
'sub disposition 2':'sub_disposition_2',
'dialer disposition':'dialer_disposition',
'call transfer from':'call_transfer_from',
'call transfer to':'call_transfer_to',
'call transfer type':'call_transfer_type',
'call response':'call_response',
'reminder date':'reminder_date',
'committed amount':'committed_amount',
'role':'role',
'recording_link':'recording_link'}, inplace=True)

date=f"{YYYY}{MM}{dd}"
target_loc =
df.to_parquet("",index=False)



######################
df.rename(columns={'Campaign Name':'campaign_name',
'Campaign ID':'campaign_id',
'Loan ID':'loan_id',
'DPD Bucket':'dpd_bucket',
'Allocation Month':'allocation_month',
'call type':'call_type',
'Agent Name':'agent_name',
'Agent Email ID':'agent_email_id',
'DID Number':'did_number',
'Customer Contact Number':'cust_contact_num',
'Shoot ID':'shoot_id',
'Call Start Time':'call_start_time',
'Customer Leg Ringing Start Time':'cust_lr_st',
'Customer Leg Ringing End Time':'cust_lr_et',
'Agent Leg Ringing Start Time':'agent_lr_st',
'Agent Leg Ringing End Time':'agent_lr_et',
'Total Ringing Time Duration':'total_ring_time_duration',
'Customer Call Pickup Time':'cust_call_pickup_time',
'Agent Call Pickup Time':'agent_call_pickup_time',
'Call End Time':'call_end_time',
'Total Talk Time Duration':'tot_talk_time_dur',
'Total Call Duration':'tot_call_dur',
'Customer Wait Duration':'cust_wait_dur',
'Wrapup Start Time':'wrapup_start_time',
'Wrapup End Time':'wrapup_end_time',
'Wrapup Duration':'wrapup_duration',
'Call Disconnected By':'call_disconnected_by',
'Call Disconnection Stage':'call_disconnection_stage',
'Call Status':'call_status',
'Disposition':'disposition',
'Sub Disposition 1':'sub_disposition_1',
'Sub Disposition 2':'sub_disposition_2',
'Dialer Disposition':'dialer_disposition',
'Call Transfer From':'call_transfer_from',
'Call Transfer To':'call_transfer_to',
'Call Transfer Type':'call_transfer_type',
'Call Response':'call_response',
'Reminder Date':'reminder_date',
'Committed Amount':'committed_amount',
'Role':'role',
'recording_link':'recording_link'}, inplace=True)

'''
CALL_DETAIL_REPORT_calling_call_details_2023-12-22-08:33_AM.csv', 'CALL_DETAIL_REPORT_calling_call_details_2023-12-23-08:33_AM.csv', 'CALL_DETAIL_REPORT_calling_call_details_2023-12-24-08:33_AM.csv', 'CALL_DETAIL_REPORT_calling_call_details_2023-12-25-08:33_AM.csv', 'CALL_DETAIL_REPORT_calling_call_details_2023-12-26-08:33_AM.csv', 'CALL_DETAIL_REPORT_calling_call_details_2023-12-27-08:33_AM.csv
'''



for file in all_files:
    print(file)
    file_name = file.get('Key')
    if pattern.match(file_name):
        print(f'File found for date: {day}, file name: {file_name}')
        file_loc = f"{s3_loc}/{file_name}"
        print(file_loc)
        df = pd.read_csv(file_loc)
        df=rename_columns(df)
        save_move_file()

        print(df.shape)

date = datetime.now().date()
dd = date.day
MM = date.month
YYYY = date.year

file_name_pattern = f"CALL_DETAIL_REPORT_calling_call_details_{YYYY}-{MM}-{dd}-*.csv"

file_name_pattern = f"CALL_DETAIL_REPORT_calling_call_details_{YYYY}-{MM}-{dd}-[0-9]{{2}}:[0-9]{{2}}_(AM|PM).csv"
# CALL_DETAIL_REPORT_calling_call_details_2023-12-23-08:33_AM.csv
pattern = re.compile(file_name_pattern)





day = '2023-12-21'
    # TODO for testing
    date = datetime.strptime(day, '%Y-%m-%d')
    dd = date.day
    mm = date.month
    YYYY = date.year

    file_name_pattern = f"CALL_DETAIL_REPORT_calling_call_details_{YYYY}-{mm}-{dd}-[0-9]{{2}}:[0-9]{{2}}_(AM|PM).csv"
    # For testing
    file_name_pattern = f"credgenics/CDR_calling_call_details_{YYYY}-{mm}-{dd}-[0-9]{{2}}:[0-9]{{2}}_(AM|PM).csv"
    # CALL_DETAIL_REPORT_calling_call_details_2023-12-23-08:33_AM.csv
    pattern = re.compile(file_name_pattern)

src_key = 'credgenics/CDR_calling_call_details_2023-12-21-01:01_PM.csv'
# buck_name = "mbk-client-sftp-prod"
src_key = 'credgenics/CDR_calling_call_details_2023-12-21-01:01_PM.csv'

todays_files = [file for file in all_files if pattern.match(file.get('Key'))]
print(file)

for file in todays_files:

file_name = "CDR_calling_call_details_2023-12-21-01:01_PM.csv"



