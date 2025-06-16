# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "920a12cc-7104-4013-a2a3-d7baa57e9e3f",
# META       "default_lakehouse_name": "lh_raw",
# META       "default_lakehouse_workspace_id": "33535eb8-4d07-49bc-b3a5-cc91d3aa6ced",
# META       "known_lakehouses": [
# META         {
# META           "id": "920a12cc-7104-4013-a2a3-d7baa57e9e3f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests
import time
import json
from notebookutils import mssparkutils

# === Step 1: Retrieve API token from Azure Key Vault ===
key_vault_uri = "https://KV-DC1Dev-ADF.vault.azure.net/"
secret_name = "splunk-token-secret"

splunk_token = mssparkutils.credentials.getSecret(
    key_vault_uri,
    secret_name
)

if not splunk_token:
    raise ValueError("Splunk API token not found in Azure Key Vault.")

print("✅ Retrieved Splunk API token successfully.")

# === Step 2: Submit the Splunk search job ===
splunk_username = "splunkcloud-api"
app = "search"
search_url = f"https://harvardmc.splunkcloud.com:8089/servicesNS/{splunk_username}/{app}/search/jobs"

headers = {
    "Authorization": f"Bearer {splunk_token}"
}

payload = {
    "search": "search index=_internal | head 5",
    "output_mode": "json"
}

response = requests.post(search_url, headers=headers, data=payload)

if response.status_code != 201:
    raise Exception(f"❌ Error starting search job: {response.status_code} - {response.text}")

job_id = response.json().get("sid")
print(f"✅ Search job started. SID: {job_id}")

# === Step 3: Poll for completion ===
status_url = f"https://harvardmc.splunkcloud.com/services/search/jobs/{job_id}"
results_url = f"{status_url}/results?output_mode=json"

while True:
    status_response = requests.get(status_url, headers=headers)
    job_content = status_response.json()

    is_done = job_content.get("entry", [{}])[0].get("content", {}).get("isDone", False)

    if is_done:
        print("✅ Search complete. Retrieving results...")
        break

    print("⏳ Waiting for search to complete...")
    time.sleep(2)

# === Step 4: Retrieve results ===
results_response = requests.get(results_url, headers=headers)

if results_response.status_code != 200:
    raise Exception(f"❌ Error fetching results: {results_response.status_code} - {results_response.text}")

results = results_response.json()
print("🔎 Search Results:")
print(json.dumps(results, indent=2))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
