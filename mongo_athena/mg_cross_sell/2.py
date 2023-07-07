from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
import pendulum

from pymongo import MongoClient
from datetime import datetime, timedelta
import pandas as pd
import uuid

_mongo_host = '10.10.150.149'
_mongo_user = 'analytics'
_mongo_password = '3Fgs6&hs1cvbHjkk'
_mongo_port = 27017
_collection = 'kyc_errors_v4'
_mongo_db = 'mobikwik'

metadata = {
    "schema": "mg_mobikwik",
    "table": "kyc_errors_v4",
    "bucket": "mbk-nifi-landingzone",
    "datapath": "/data/"}


def get_mongo_connection():
    try:
        if _mongo_host and _mongo_db and _collection and _mongo_port and _mongo_user and _mongo_password:
            client = MongoClient(_mongo_host, _mongo_port, username=_mongo_user, password=_mongo_password,
                                 authSource=_mongo_db)
            db = client[_mongo_db]
            _coll = db[_collection]
            client.server_info()
    except Exception as e:
        print(e)
    return _coll


def save_data(df):
    data_path = "s3://" + metadata['bucket'] + metadata['datapath'] + "mg_" + _mongo_db + "/" + _collection + "/"
    uq_id = str(uuid.uuid4())
    data_path = data_path + uq_id + ".parquet"
    print("Saving File to location: {}".format(data_path))
    df.to_parquet(data_path, index=False)


def data_prep(df):
    #df.fillna('', inplace=True)
    # Removing Millisecs from datetime
    df['obj_id'] = df['obj_id'].astype('str')
    return df


def extract_policy_data(start_day, end_day):
    print(f"Date range is: {start_day} to {end_day}")
    _coll = get_mongo_connection()
    # Need these columns, projection used for creating aliases
    proj = {"$project": {
        "obj_id": "$_id",
        "member_id": "$memberId",
        "insurance_category": "$insuranceCategory",
        "insurance_sell_platform": "$insuranceSellPlatform",
        "insurer": "$insurer",
        "amount_paid": "$amountPaid",
        "cross_sell": "$crossSell",
        "booked_at": "$bookedAt",##**
        "debited_at": "$debitedAt",
        "created_at": "$createdAt",
        "last_modified": "$lastModified"}}
    query = {"$match": {audit_col: {"$gte": start_day, "$lt": end_day}}}
    pipeline = [proj]
    mongo_data = _coll.aggregate(pipeline)
    arr = []
    for data in mongo_data:
        arr.append(data)
    df = pd.DataFrame(arr)
    print("Data Extracted, Row count is {}".format(df.shape[0]))
    df = data_prep(df)
    save_data(df)



    cols=['obj_id', 'full_name', 'member_id', 'contact_no', 'city', 'state', 'kycrequest_status', 'v4_errors', 'created_at', 'updated_at']

    pipeline=[query, proj]


#required columns would be -
# _id, amountpaid, bookedat, createdat, crosssell, debitedat,insurancecategory, insurancesellplatform, insurer,lastmodified,memberid,

# "insuranceId" : "INSE0D668J30H205F", "memberId" : "yd1078@gmail.com", "primaryCell" : "9694792804", "walletTransactionId" : "946574178", "crossSell" : false, "client" : "android", "ip" : "106.79.200.184, 172.70.90.84, 10.10.145.179", "insuranceSellPlatform" : "IMPS_CROSS_SELL", "insurer" : "ONEASSIST", "status" : "INCOMPLETE_DETAILS", "insuranceCategory" : "WALLET_PROTECT", "amountPaid" : 79, "sumAssured" : "150000", "sumAssuredUnit" : "", "userAppDetails" : { "userClientType" : "Android", "subClientType" : "Invalid", "appVersion" : "1809" }, "tenureInMonths" : 1, "masterPolicyNo" : "", "masterCode" : "", "kyc" : false, "debitedAt" : ISODate("2023-06-01T00:15:36.605Z"), "validFrom" : ISODate("2023-05-31T18:30:00Z"), "validUpto" : ISODate("2023-06-30T18:29:59.999Z"), "customerDetails" : { "kycType" : "E-aadhaar xml Kyc", "name" : "Yagya Dutta Verma", "gender" : "M", "genderE" : "MALE", "dob" : "10-01-1978", "email" : "yd1078@gmail.com", "mobile" : "9694792804", "address" : " C.I.S.F. Road Deoli Tonk Rajasthan 304804", "state" : "Rajasthan", "pincode" : "304804", "nomineeName" : "", "nomineeAge" : "", "nomineeGender" : "", "nomineeRelationship" : "", "additionalInfo" : {  } }, "wapgTxnInfo" : { "pgStatus" : false, "pgAmount" : 0, "agId" : "INS-POLMBK1IB6ADJ0F" }, "policyIconUrl" : "https://static.mobikwik.com/appdata/revamp/insurance/oneassist_icon.png", "responseType" : "INCOMPLETE_DETAILS", "sent" : false, "purchaseResponse" : {  }, "autoRenew" : false, "autoPurchase" : false, "userChoseToSkipNominee" : false, "userCrossSellRandomizationId" : "64779e0746e0fb0038c88cee", "notificationData" : { "smsCount" : 0, "appNotificationCount" : 0, "whatsAppNotificationCount" : 0, "totalNotifications" : 0, "notificationRecords" : [ ] }, "errorDetailsList" : [ ], "source" : "API", "version" : NumberLong(3), "createdAt" : ISODate("2023-06-01T00:15:36.317Z"), "lastModified" : ISODate("2023-06-01T00:15:38.641Z") }
#{ "_id" : "POLMBKD6C8A26B7", "_class" : "com.mobikwik.crosssell.insurance.policy.entities.PolicyEntity", "insuranceId" : "INSC1HPJ55JIAGI09", "memberId" : "8600015416@nocash.mobikwik.com", "primaryCell" : "8600015416", "walletTransactionId" : "946585055", "crossSell" : false, "client" : "android", "ip" : "106.216.240.159, 172.70.218.23, 10.10.145.7", "insuranceSellPlatform" : "IMPS_CROSS_SELL", "insurer" : "HEALTH_ASSURE", "status" : "PURCHASED", "insuranceCategory" : "ASSURE_PLUS", "amountPaid" : 117, "sumAssured" : "100000", "sumAssuredUnit" : "", "userAppDetails" : { "userClientType" : "Android", "subClientType" : "Invalid", "appVersion" : "1821" }, "tenureInMonths" : 12, "masterPolicyNo" : "", "masterCode" : "", "kyc" : false, "bookedAt" : ISODate("2023-06-01T01:10:03.682Z"), "debitedAt" : ISODate("2023-06-01T01:09:55.251Z"), "validFrom" : ISODate("2023-05-31T18:30:00Z"), "validUpto" : ISODate("2024-05-31T18:29:59.999Z"), "policyDispatchTime" : ISODate("2023-06-01T01:10:03.693Z"), "policyDocUrl" : "https://promotions.mobikwik.com/inapp/terms-and-conditions/wallet-assist/files/healthassure-coi.pdf", "customerDetails" : { "kycType" : "E-aadhaar xml Kyc", "name" : "Suhas Chandrakant Sheth", "gender" : "M", "genderE" : "MALE", "dob" : "07-03-1976", "email" : "8600015416@nocash.mobikwik.com", "mobile" : "8600015416", "address" : "- Flat 305 Plot 508/3 Kamleshwari Park Near Mahalaxmi Sabhagruha Parvati Pune City Pune Maharashtra 411009", "state" : "Maharashtra", "pincode" : "411009", "nomineeName" : "", "nomineeAge" : "", "nomineeGender" : "", "nomineeRelationship" : "", "additionalInfo" : {  } }, "wapgTxnInfo" : { "pgStatus" : false, "pgAmount" : 0, "agId" : "INS-POLMBKD6C8A26B7" }, "policyIconUrl" : "https://static.mobikwik.com/appdata/revamp/insurance/healthassureicon.png", "responseType" : "PURCHASED", "sent" : false, "purchaseResponse" : { "message" : "Saved Sucessfully", "code" : "200" }, "autoRenew" : false, "autoPurchase" : false, "userChoseToSkipNominee" : false, "userCrossSellRandomizationId" : "64618c2d46e0fb0037a1cc13", "notificationData" : { "smsCount" : 0, "appNotificationCount" : 0, "whatsAppNotificationCount" : 0, "totalNotifications" : 0, "notificationRecords" : [ ] }, "errorDetailsList" : [ ], "source" : "API", "version" : NumberLong(5), "createdAt" : ISODate("2023-06-01T01:09:54.842Z"), "lastModified" : ISODate("2023-06-01T01:10:03.693Z") }
