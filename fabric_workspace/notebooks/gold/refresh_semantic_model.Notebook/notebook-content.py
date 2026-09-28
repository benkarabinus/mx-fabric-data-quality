# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1a2b3c4d-0003-4000-8000-000000000003",
# META       "default_lakehouse_name": "mx_gold",
# META       "default_lakehouse_workspace_id": "1a2b3c4d-0000-4000-8000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "1a2b3c4d-0003-4000-8000-000000000003"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## refresh_semantic_model
#
# Frames the `mx_data_quality` Direct Lake model against the gold tables that
# `gold_score_rules` just wrote, so the report shows this run's numbers.
#
# ### Why this step exists
#
# Direct Lake is often described as needing no refresh: change the Delta files and the
# model picks them up. That is true, but only **after the model has been framed once**.
#
# A model published over the REST API has never been framed. It has no partition state,
# so there is nothing for an incremental reframe to update. In that state the refresh
# path the workspace Refresh button uses is rejected during validation with:
#
# > Unable to load a query that produces no tables
#
# and - this is what makes it hard to diagnose - **no refresh-history entry is written**,
# because the refresh is rejected before one is created. An empty refresh history means
# "never started", not "never attempted".
#
# The *enhanced* refresh API accepts the same model and frames it. Once one framing has
# succeeded, every later path works, including the Refresh button. This notebook issues
# that call on every run, so a new workspace needs no manual step and later runs stay
# current.
#
# ### What it does not do
#
# It does not reload data. Direct Lake reads the Delta files where they lie; framing
# only points the model's partitions at the current files.

# CELL ********************

import json
import time

import sempy.fabric as fabric

MODEL_NAME = "mx_data_quality"

# How long to wait for framing before giving up. Framing is metadata-only and
# normally finishes in seconds; this bound exists so a stuck service cannot
# hold the pipeline open for the activity's full twelve-hour timeout.
TIMEOUT_SECONDS = 600
POLL_SECONDS = 5

# sempy supplies a client already authenticated as the identity running this
# notebook. Refresh lives on the Power BI endpoint, not the Fabric one.
client = fabric.PowerBIRestClient()

workspace_id = fabric.get_workspace_id()
print(f"workspace {workspace_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Resolve the model by name rather than by ID. The ID is assigned when the
# model is published and is different in every workspace, so binding to it
# would mean threading another placeholder through the deployment. The name
# is ours and is stable.
datasets = client.get(f"v1.0/myorg/groups/{workspace_id}/datasets").json()["value"]
matches = [d for d in datasets if d["name"] == MODEL_NAME]

if not matches:
    found = ", ".join(sorted(d["name"] for d in datasets)) or "none"
    raise RuntimeError(
        f"Semantic model '{MODEL_NAME}' was not found in this workspace. "
        f"Models present: {found}. Deploy the model before running this pipeline."
    )

dataset_id = matches[0]["id"]
print(f"model {MODEL_NAME} = {dataset_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The enhanced refresh endpoint. It shares a URL with the legacy one and is
# selected by the body: a legacy call sends only notifyOption. Omitting
# "objects" refreshes the whole model, which is what framing needs.
#
# commitMode "transactional" means the model either ends up framed against
# this run's files or unchanged. It never half-frames, so the report cannot
# show one table from this run beside another from the last.
body = {
    "type": "Full",
    "commitMode": "transactional",
    "retryCount": 2,
}

response = client.post(
    f"v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes",
    json=body,
)

# The service returns the new refresh's ID in a header, so we can poll that
# refresh specifically rather than whatever happens to be newest. Header
# casing varies; take whichever is present and fall back to the newest entry.
request_id = response.headers.get("RequestId") or response.headers.get("requestid")
print(f"refresh accepted (HTTP {response.status_code}), request {request_id or 'unknown'}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Poll rather than return immediately. A pipeline activity that reports success
# the moment the request is accepted would let a failed framing pass unnoticed,
# and the report would quietly serve the previous run's numbers.
#
# "Unknown" is the service's word for in progress.
deadline = time.monotonic() + TIMEOUT_SECONDS
status = "Unknown"
latest = {}

refreshes_url = f"v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes"

while time.monotonic() < deadline:
    if request_id:
        latest = client.get(f"{refreshes_url}/{request_id}").json()
        status = latest.get("status", "Unknown")
    else:
        history = client.get(f"{refreshes_url}?$top=1").json()["value"]
        if history:
            latest = history[0]
            status = latest.get("status", "Unknown")

    if status != "Unknown":
        break

    time.sleep(POLL_SECONDS)

print(f"status {status}")

if status == "Completed":
    # The per-table detail is only present on the single-refresh response, and
    # only for enhanced refreshes. Print it when we have it: it is the proof
    # that framing reached every table rather than stopping at the first.
    for obj in latest.get("objects", []):
        print(f"  {obj.get('table')}: {obj.get('status')}")
    print(f"{MODEL_NAME} framed against the current gold tables")
elif status == "Unknown":
    raise RuntimeError(
        f"Refresh of '{MODEL_NAME}' did not finish within {TIMEOUT_SECONDS}s. "
        "It may still be running; check the model's refresh history."
    )
else:
    # serviceExceptionJson carries the real reason. Surface it rather than the
    # bare status, which on its own says nothing actionable.
    detail = latest.get("serviceExceptionJson") or json.dumps(latest, indent=2)
    raise RuntimeError(f"Refresh of '{MODEL_NAME}' ended as {status}: {detail}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
