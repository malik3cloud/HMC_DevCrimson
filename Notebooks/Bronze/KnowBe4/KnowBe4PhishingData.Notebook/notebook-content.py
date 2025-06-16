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

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit, to_timestamp,current_timestamp
from pyspark.sql.types import StringType, TimestampType
import requests
import pandas as pd
import time
import json
import os
from pyspark.sql import SparkSession
from py4j.java_gateway import java_import
# ------------------------------------------
# Setup Spark
# ------------------------------------------
spark = SparkSession.builder.getOrCreate()

# ------------------------------------------
# Auth / Config
# ------------------------------------------
base_url = "https://us.api.knowbe4.com/v1/phishing/security_tests"
bearer_token = mssparkutils.credentials.getSecret('https://KV-DC1Dev-ADF.vault.azure.net/', 'knowbe4-token')
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Accept": "application/json"
}
params = {"page": 1, "per_page": 500}
REQUESTS_PER_MINUTE = 45
SECONDS_PER_REQUEST = 60 / REQUESTS_PER_MINUTE
api_call_count = 0
TEST_MODE = False  # 🔁 Set to False to process all recipients

# ------------------------------------------
# 1. Fetch Security Test Metadata
# ------------------------------------------
all_tests = []
while True:
    response = requests.get(base_url, headers=headers, params=params)
    api_call_count += 1
    response.raise_for_status()
    page_data = response.json()
    if not page_data:
        break
    all_tests.extend(page_data)
    if len(page_data) < params["per_page"]:
        break
    params["page"] += 1

test_df = spark.createDataFrame(pd.DataFrame(all_tests))
test_df = test_df.withColumnRenamed("landing-page", "landing_page")

test_df = test_df \
    .withColumn("groups", concat_ws(",", col("groups.name"))) \
    .withColumn("categories", concat_ws(",", col("categories.name"))) \
    .withColumn("template_name", col("template.name")) \
    .withColumn("landing_page_name", col("landing_page.name")) \
    .drop("template", "landing_page")

test_df.coalesce(1).write.mode("overwrite").saveAsTable("PhishingSecurityTests")

# ------------------------------------------
# 2. Pick Latest pst_id Based on started_at
# ------------------------------------------
test_df = test_df.withColumn("started_at_ts", to_timestamp(col("started_at")))
latest_test_row = test_df.orderBy(col("started_at_ts").desc()).limit(1).collect()[0]
pst_id = latest_test_row["pst_id"]
print(f"🆕 Automatically selected latest pst_id={pst_id} based on started_at={latest_test_row['started_at']}")

# ------------------------------------------
# 3. Fetch Recipients for Selected pst_id
# ------------------------------------------
recipient_ids_to_fetch = []

# Fetch only one recipient for test mode
url = f"https://us.api.knowbe4.com/v1/phishing/security_tests/{pst_id}/recipients"
params = {"page": 1, "per_page": 1 if TEST_MODE else 500}

while True:
    response = requests.get(url, headers=headers, params=params)
    api_call_count += 1

    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        print(f"Rate limited. Sleeping {retry_after} seconds")
        time.sleep(retry_after)
        continue

    response.raise_for_status()
    recipients = response.json()
    if not recipients:
        break

    for r in recipients:
        recipient_ids_to_fetch.append((pst_id, r["recipient_id"]))

    if TEST_MODE or len(recipients) < 500:
        break
    params["page"] += 1
    time.sleep(SECONDS_PER_REQUEST)

print(f"🔬 Test mode: {TEST_MODE} | Recipients to fetch: {len(recipient_ids_to_fetch)}")

# ------------------------------------------
# 4. Fetch Recipient Details
# ------------------------------------------
detailed_recipient_data = []

for pst_id, recipient_id in recipient_ids_to_fetch:
    detail_url = f"https://us.api.knowbe4.com/v1/phishing/security_tests/{pst_id}/recipients/{recipient_id}"
    try:
        response = requests.get(detail_url, headers=headers)
        api_call_count += 1

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"[Detail] Rate limited. Sleeping {retry_after} seconds")
            time.sleep(retry_after)
            continue

        response.raise_for_status()
        data = response.json()
        data["pst_id"] = pst_id
        data["recipient_id"] = recipient_id
        detailed_recipient_data.append(data)

        time.sleep(SECONDS_PER_REQUEST)

    except Exception as e:
        print(f"Error fetching pst_id={pst_id}, recipient_id={recipient_id}: {str(e)}")

# ------------------------------------------
# 5. Clean and Convert
# ------------------------------------------
print(f"Raw detailed records: {len(detailed_recipient_data)}")

cleaned_data = []
dropped_count = 0

for r in detailed_recipient_data:
    if not isinstance(r, dict):
        dropped_count += 1
        continue
    if "user" not in r or "template" not in r:
        dropped_count += 1
        continue
    cleaned_data.append(r)

print(f"Cleaned records: {len(cleaned_data)} | Dropped: {dropped_count}")

detailed_pdf = pd.DataFrame(cleaned_data)
detailed_df = spark.createDataFrame(detailed_pdf)

# Fix null-type columns
for col_name in detailed_df.columns:
    if detailed_df.schema[col_name].dataType.simpleString() == "void":
        detailed_df = detailed_df.withColumn(col_name, lit("").cast(StringType()))

# Cast timestamp fields
timestamp_cols = [
    "scheduled_at", "delivered_at", "opened_at", "clicked_at", "replied_at",
    "attachment_opened_at", "macro_enabled_at", "data_entered_at",
    "qr_code_scanned", "reported_at", "bounced_at"
]
for col_name in timestamp_cols:
    if col_name in detailed_df.columns:
        detailed_df = detailed_df.withColumn(col_name, col(col_name).cast(TimestampType()))

# Flatten user and template
detailed_df = detailed_df \
    .withColumn("user_id", col("user.id")) \
    .withColumn("user_first_name", col("user.first_name")) \
    .withColumn("user_last_name", col("user.last_name")) \
    .withColumn("user_email", col("user.email")) \
    .withColumn("template_id", col("template.id")) \
    .withColumn("template_name", col("template.name")) \
    .drop("user", "template")

# ------------------------------------------
# 6. Join with Metadata and Write Output
# ------------------------------------------
# Initialize Spark session
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# Define paths
base_path = "Files/IncomingFeed/CyberSecurityDashboard/KnowBe4"
output_path = f"{base_path}/output"
final_filename = "knowbe4_data.parquet"

# Initialize Hadoop FileSystem
hadoop_conf = sc._jsc.hadoopConfiguration()
fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)

# Java imports for Path
java_import(sc._jvm, "org.apache.hadoop.fs.Path")

# Define Path objects
output_path_obj = sc._jvm.Path(output_path)
final_file_path = sc._jvm.Path(f"{base_path}/{final_filename}")

# Delete existing temporary output directory if it exists
if fs.exists(output_path_obj):
    fs.delete(output_path_obj, True)

# Join detailed_df with test_df and add ETLLoadDateTime column
final_df = detailed_df.join(
    test_df.drop("template_name", "started_at_ts"), 
    on="pst_id", 
    how="inner"
)

# Write DataFrame to temporary output directory
final_df.coalesce(1).write.mode("overwrite").parquet(output_path)

# Delete existing final file if it exists
if fs.exists(final_file_path):
    fs.delete(final_file_path, False)

# Rename the part file to the final filename
file_status = fs.listStatus(output_path_obj)
for f in file_status:
    name = f.getPath().getName()
    if name.endswith(".parquet"):
        part_file = f.getPath()
        fs.rename(part_file, final_file_path)
        break

# Delete the temporary output directory
fs.delete(output_path_obj, True)

# ------------------------------------------
# 7. Log API usage
# ------------------------------------------
print(f"✅ Done for pst_id={pst_id}")
print(f"🔢 Total API calls made: {api_call_count}")
print(f"👥 Total recipients processed: {len(recipient_ids_to_fetch)}")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
