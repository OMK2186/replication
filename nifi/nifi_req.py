
python -m pip install nipyapi

import nipyapi
#url_nifi_api="https://nifi.analytics.mbkinternal.in/nifi-api/"
url_nifi_api='http://localhost:8081/nifi-api'
nipyapi.config.nifi_config.host = url_nifi_api
nipyapi.canvas.get_root_pg_id()

nipyapi.config.registry_config.host = 'http://localhost:18080/nifi-registry-api'

###############
import requests
import json
from .get_processor import get_processor
def update_processor_status(processor_id: str, new_state: str, url_nifi_api):
    #processor = get_processor(url_nifi_api, processor_id, token)
    put_dict = {"revision": {"clientId": "b9118b51-0185-1000-a982-ad165d46b1df","version": 8}, "state": new_state,"disconnectedNodeAcknowledged": True,}
    payload = json.dumps(put_dict).encode("utf8")
    header = {
        "Content-Type": "application/json"}
    response = requests.put(
        url_nifi_api + f"processors/{processor_id}/run-status",
        headers=header,
        data=payload,
        verify=False
    )
    return response


re=update_processor_status(url_nifi_api="https://nifi.analytics.mbkinternal.in/nifi-api/", processor_id="b91fe430-0185-1000-3cbe-5a53bd215eac", new_state="RUNNING")

re=update_processor_status(url_nifi_api="https://nifi.analytics.mbkinternal.in/nifi-api/", processor_id="b9134581-0185-1000-47cd-7dc206619677", new_state="RUNNING")


print(re)
#https://nifi.analytics.mbkinternal.in:443/nifi-api/processors/b91fe430-0185-1000-3cbe-5a53bd215eac


https://nifi.analytics.mbkinternal.in/nifi-api/processors/b91fe430-0185-1000-3cbe-5a53bd215eac

import requests
import json
def get_processor_state(url_nifi_api: str, processor_id: str):
    header = {"Content-Type": "application/json"}
    response = requests.get(
        url_nifi_api + f"processors/{processor_id}/state", headers=header, verify=False
    )
    return json.loads(response.content)

def get_processor_state2(url_nifi_api: str, processor_id: str):
    header = {"Content-Type": "application/json"}
    response = requests.get(
        url_nifi_api + f"processors/{processor_id}/run-state", headers=header, verify=False
    )
    return response


#re=get_processor_state("localhost:9443/nifi-api/", "b91fe430-0185-1000-3cbe-5a53bd215eac")


re=get_processor_state2("https://nifi.analytics.mbkinternal.in/nifi-api/", "b91fe430-0185-1000-3cbe-5a53bd215eac")
print(re)

#https://nifi.analytics.mbkinternal.in/nifi-api/processors/b91fe430-0185-1000-3cbe-5a53bd215eac/run-status



#######################################
>>> import requests
>>> import json
>>> def get_processor_state(url_nifi_api: str, processor_id: str):
...     header = {"Content-Type": "application/json"}
...     response = requests.get(
...         url_nifi_api + f"processors/{processor_id}/state", headers=header
...     )
...     return json.loads(response.content)
... 
>>> import requests
>>> import json
>>> def get_processor_state(url_nifi_api: str, processor_id: str):
...     header = {"Content-Type": "application/json"}
...     response = requests.get(
...         url_nifi_api + f"processors/{processor_id}/state", headers=header, verify=False
...     )
...     return json.loads(response.content)
... 
>>> re=get_processor_state("https://nifi.analytics.mbkinternal.in/nifi-api/", "b91fe430-0185-1000-3cbe-5a53bd215eac")

/usr/lib/python3/dist-packages/urllib3/connectionpool.py:1015: InsecureRequestWarning: Unverified HTTPS request is being made to host 'nifi.analytics.mbkinternal.in'. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings
  warnings.warn(
>>> 
>>> print(re)
{'componentState': {'componentId': 'b91fe430-0185-1000-3cbe-5a53bd215eac', 'stateDescription': 'Gives the option to store values not only on the FlowFile but as stateful variables to be referenced in a recursive manner.', 'localState': {'scope': 'LOCAL', 'totalEntryCount': 1, 'state': [{'key': 'last_tms', 'value': 'Mon Jan 16 11:28:06 IST 2023'}]}}}
>>> 


import requests
url = 'http://localhost:8001/contentListener'
data = {"username":"xyz","password":"xyz"}
data={"trigger_id": "959cb132b17a_1", "mapping_id": 1, "nifi_conn_id": "mysql_an_250", "auto_incr_col": None, "audit_col_name": "updatedat", "max_audit_val": "2001-01-01", "nifi_target_bucket": "mbk-nifi-landingzone", "batch_size": 100000, "nifi_target_loc_prefix": "replication_v2/data", "entity_src_schema": "mobinew", "entity_src_name": "memberdetail", "entity_target_schema": "mobinew", "entity_target_name": "memberdetail","nifi_target_bucket":"mbk-nifi-landingzone","nifi_target_loc_prefix":"replication_v2/data"}

requests.post(url, json=data)
>>> requests.post(nifi_url, data=nifi_metadata_req_body)

curl --request POST   --data '{"username":"xyz","password":"xyz"}'  http://localhost:8001/contentListener


# NiFi bulletin

## https://nifi.analytics.mbkinternal.in/nifi-api/flow/bulletin-board

import requests
import json

bulletin_url="https://nifi.analytics.mbkinternal.in/nifi-api/flow/bulletin-board"
requests.get(bulletin_url, headers=header, verify=False)

pro_url="https://nifi.analytics.mbkinternal.in/nifi-api/processors/ad6ed226-018a-1000-7590-a2099ef21365"
requests.get(pro_url, headers=header, verify=False)


cont_url="https://nifi.analytics.mbkinternal.in/nifi-api/controller/process-groups/1f6ba7c1-018b-1000-5a57-2b1264b52061/status?recursive=true"
cont_url="https://nifi.analytics.mbkinternal.in/nifi-api/controller/status"


# SELECT coalesce(max(${audit_col_name}),${default_max_audit_val}) as new_audit_val from ${entity_src_schema}.${entity_src_name}