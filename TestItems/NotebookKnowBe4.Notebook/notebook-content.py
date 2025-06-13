# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

connectionName = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils

# KnowBe4 Connection Name
connection_name = "KnowBe4_Connector"

# This returns a dictionary with auth info
conn = mssparkutils.credentials.getConnectionStringOrCreds(connection_name)

# Extract the token or key (depending on how the API connection is set up)
api_key = conn.get("accessToken") or conn.get("apiKey") or conn.get("key")
api_url = conn.get("url") or "https://your-api-host.com"  # If not stored, set manually

# Build the header
headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}


# response = requests.get("https://your-api-endpoint.com/data", headers=headers)

# print(response.json())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
