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

from pyspark.sql.functions import col, concat_ws
import requests
import json
import pandas as pd

# ------------------------------------------
# 0. Fetch JSON from KnowBe4 Phishing Tests API
# ------------------------------------------

base_url = "https://us.api.knowbe4.com/v1/phishing/security_tests"
bearer_token = "eyJhbGciOiJIUzUxMiJ9.eyJzaXRlIjoidHJhaW5pbmcua25vd2JlNC5jb20iLCJ1dWlkIjoiYTBlY2I0NzEtZTNlZC00NjZjLTgwYTYtYjA3MWVlY2UxYzRmIiwic2NvcGVzIjpbImVsdmlzIl0sImFpZCI6MTYxODJ9.fvtu6aS8REg9NhBCfI_zUMacfPIhqBloyearPXpNQJDB_NLBSyRl1RusHkg3hVp1UOe7Mv2Tee6pfRiDHNA23w"  # Replace with actual token or retrieve securely
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Accept": "application/json"
}
params = {"page": 1, "per_page": 500}

all_tests = []
while True:
    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()
    page_data = response.json()

    if not page_data:
        break

    all_tests.extend(page_data)
    if len(page_data) < params["per_page"]:
        break
    params["page"] += 1

# ------------------------------------------
# 1. Convert to Spark DataFrame
# ------------------------------------------
pdf = pd.DataFrame(all_tests)
df = spark.createDataFrame(pdf)

# ------------------------------------------
# 2. Clean and Flatten Structs
# ------------------------------------------
# Rename problematic fields
df = df.withColumnRenamed("landing-page", "landing_page")

# Extract nested fields and flatten arrays
df = df.withColumn("groups", concat_ws(",", col("groups.name"))) \
       .withColumn("categories", concat_ws(",", col("categories.name"))) \
       .withColumn("template_name", col("template.name")) \
       .withColumn("landing_page_name", col("landing_page.name"))

# Drop unsupported struct columns
df = df.drop("template", "landing_page")

# ------------------------------------------
# 3. Write to Lakehouse Table
# ------------------------------------------
df.coalesce(1).write \
    .mode("overwrite") \
    .saveAsTable("PhishingSecurityTests")

# ------------------------------------------
# 4. Fetch Recipients for Each Security Test
# ------------------------------------------

recipient_base_url = "https://us.api.knowbe4.com/v1/phishing/security_tests/{pst_id}/recipients"

# Collect pst_id values
pst_ids = [row["id"] for row in df.select("id").distinct().collect()]

recipient_data = []

for pst_id in pst_ids:
    page = 1
    per_page = 500

    while True:
        url = recipient_base_url.format(pst_id=pst_id)
        params = {"page": page, "per_page": per_page}
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        recipients = response.json()

        if not recipients:
            break

        for recipient in recipients:
            recipient["pst_id"] = pst_id  # Add reference to the parent test

        recipient_data.extend(recipients)

        if len(recipients) < per_page:
            break

        page += 1
        
# ------------------------------------------
# 5. Convert to DataFrame and Save
# ------------------------------------------

# Convert to pandas, then Spark DataFrame
recipients_pdf = pd.DataFrame(recipient_data)
recipients_df = spark.createDataFrame(recipients_pdf)

# Write to Lakehouse table
recipients_df.coalesce(1).write \
    .mode("overwrite") \
    .saveAsTable("PhishingRecipients")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
