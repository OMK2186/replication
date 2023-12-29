__author__ = 'ketankk'
# https://nifi.analytics.mbkinternal.in/nifi/?processGroupId=ad6e7a6e-018a-1000-9e39-8a0b7b170da2&componentIds=
import requests

nifi_url = 'http://localhost:8001/contentListener'
import uuid

# An unique request id for each request, even same table getting
# triggered multiple times will have different trigger_id. This id will be used to track end to end replication,
# ie. nifi jobs different state, Glue Job, etc.
trigger_id = str(uuid.uuid4().hex[:12])
nifi_conn_id = "mbk_an_slave"
src_schema_name = "mobinew"
src_table_name = "memberdetail"
target_schema_name = "mobinew_hudi"
target_table_name = "memberdetail"


# The name of a column whose values will be used for partitioning. The default behavior is to use row numbers on the
# result set for partitioning into 'pages' to be fetched from the database, using an offset/limit strategy. However
# for certain databases, it can be more efficient under the right circumstances to use the column values themselves
# to define the 'pages'. This property should only be used when the default queries are not performing well,
# when there is no maximum-value column or a single maximum-value column whose type can be coerced to a long integer
# (i.e. not date or timestamp), and the column values are evenly distributed and not sparse, for best performance.

auto_incr_col = ""
audit_col_name = "updatedat"
max_audit_val = "2023-09-01"
batch_size = "200000"

nifi_metadata = {
    "trigger_id": trigger_id,
    "nifi_conn_id": nifi_conn_id,
    "src_schema_name": src_schema_name,
    "src_table_name": src_table_name,
    "target_schema_name": target_schema_name,
    "target_table_name": target_table_name,
    "auto_incr_col": auto_incr_col,
    "audit_col_name": audit_col_name,
    "max_audit_val": max_audit_val,
    "batch_size": batch_size,

}

nifi_url = 'http://localhost:8083/replicationV2'
requests.post(nifi_url, json=nifi_metadata)



import uuid
str(uuid.uuid4().hex[:12])
#'525c2a3833f6'
