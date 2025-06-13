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
# META     },
# META     "environment": {
# META       "environmentId": "0a2e55e8-f579-b73b-4a24-a95f3abe0690",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, concat_ws
import requests
import json
import os

# ------------------------------------------
# 0. Fetch JSON from KnowBe4 API and save
# ------------------------------------------

# API config
base_url = "https://us.api.knowbe4.com/v1/users"
bearer_token = "eyJhbGciOiJIUzUxMiJ9.eyJzaXRlIjoidHJhaW5pbmcua25vd2JlNC5jb20iLCJ1dWlkIjoiYTBlY2I0NzEtZTNlZC00NjZjLTgwYTYtYjA3MWVlY2UxYzRmIiwic2NvcGVzIjpbImVsdmlzIl0sImFpZCI6MTYxODJ9.fvtu6aS8REg9NhBCfI_zUMacfPIhqBloyearPXpNQJDB_NLBSyRl1RusHkg3hVp1UOe7Mv2Tee6pfRiDHNA23w"  # Replace with actual token or retrieve securely
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Accept": "application/json"
}
params = {
    "page": 1,
    "per_page": 500
}

all_users = []
while True:
    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()

    users_page = response.json()
    if not users_page:
        break

    all_users.extend(users_page)
       
    if len(users_page) < params["per_page"]:
        break  # No more pages
    params["page"] += 1

# Save to Lakehouse path
json_output_path = "/lakehouse/default/Files/IncomingFeed/KnowBe4Test/knowbe4users.json"
with open(json_output_path, "w", encoding="utf-8") as f:
    for user in all_users:
        f.write(json.dumps(user) + "\n")

# ------------------------------------------
# 1. Load JSON 
# ------------------------------------------
json_path_spark = "Files/IncomingFeed/KnowBe4Test/knowbe4users.json"
df = spark.read.json(json_path_spark)

# ------------------------------------------
# 2. Flatten array fields
# ------------------------------------------
df = df.withColumn("groups", concat_ws(",", col("groups"))) \
       .withColumn("aliases", concat_ws(",", col("aliases")))

# ------------------------------------------
# 3. Extract original JSON field order
# ------------------------------------------
json_path_python = "/lakehouse/default/Files/IncomingFeed/KnowBe4Test/knowbe4users.json"
with open(json_path_python, "r", encoding="utf-8-sig") as f:
    first_line = f.readline()
    data = json.loads(first_line)

ordered_columns = list(data.keys())

# Get column order from first record
if isinstance(data, list):
    ordered_columns = list(data[0].keys())
else:
    ordered_columns = list(data.keys())

# Reorder DataFrame columns
df = df.select(*ordered_columns)

# ------------------------------------------
# 4. Coalesce to single partition
# ------------------------------------------
df_single = df.coalesce(1)

# ------------------------------------------
# 5. Write directly to Lakehouse table
# ------------------------------------------
df.write.mode("overwrite").saveAsTable("KnowBe4Users")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
