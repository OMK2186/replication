
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
