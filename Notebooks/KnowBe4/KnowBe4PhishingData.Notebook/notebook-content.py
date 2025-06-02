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
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import requests
import pandas as pd
import time

# ------------------------------------------
# Setup Spark (if not already)
# ------------------------------------------
spark = SparkSession.builder.getOrCreate()

# ------------------------------------------
# Auth / Config
# ------------------------------------------
base_url = "https://us.api.knowbe4.com/v1/phishing/security_tests"
bearer_token = "eyJhbGciOiJIUzUxMiJ9.eyJzaXRlIjoidHJhaW5pbmcua25vd2JlNC5jb20iLCJ1dWlkIjoiYTBlY2I0NzEtZTNlZC00NjZjLTgwYTYtYjA3MWVlY2UxYzRmIiwic2NvcGVzIjpbImVsdmlzIl0sImFpZCI6MTYxODJ9.fvtu6aS8REg9NhBCfI_zUMacfPIhqBloyearPXpNQJDB_NLBSyRl1RusHkg3hVp1UOe7Mv2Tee6pfRiDHNA23w"
headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Accept": "application/json"
}
params = {"page": 1, "per_page": 500}
REQUESTS_PER_MINUTE = 45
SECONDS_PER_REQUEST = 60 / REQUESTS_PER_MINUTE

# ------------------------------------------
# 1. Fetch Security Test Metadata
# ------------------------------------------
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
# 2. Fetch Recipients Per Security Test
# ------------------------------------------
recipient_base_url = "https://us.api.knowbe4.com/v1/phishing/security_tests/{pst_id}/recipients"
pst_ids = [row["pst_id"] for row in test_df.select("pst_id").distinct().collect()]
pst_ids = pst_ids[:1]  # Limit for dev/testing

recipient_ids_to_fetch = []

for pst_id in pst_ids:
    page = 1
    while True:
        url = recipient_base_url.format(pst_id=pst_id)
        params = {"page": page, "per_page": 500}
        response = requests.get(url, headers=headers, params=params)

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

        if len(recipients) < 500:
            break

        page += 1
        time.sleep(SECONDS_PER_REQUEST)

    time.sleep(SECONDS_PER_REQUEST)

# ------------------------------------------
# 3. Fetch Detailed Recipient Info
# ------------------------------------------
detailed_recipient_data = []
recipient_ids_to_fetch = recipient_ids_to_fetch[:2]
for pst_id, recipient_id in recipient_ids_to_fetch:
    detail_url = f"https://us.api.knowbe4.com/v1/phishing/security_tests/{pst_id}/recipients/{recipient_id}"
    try:
        detail_response = requests.get(detail_url, headers=headers)
        if detail_response.status_code == 429:
            retry_after = int(detail_response.headers.get("Retry-After", 60))
            print(f"[Detail] Rate limited. Sleeping {retry_after} seconds")
            time.sleep(retry_after)
            continue

        detail_response.raise_for_status()
        data = detail_response.json()
        data["pst_id"] = pst_id
        data["recipient_id"] = recipient_id
        detailed_recipient_data.append(data)

        time.sleep(SECONDS_PER_REQUEST)

    except Exception as e:
        print(f"Error fetching pst_id={pst_id}, recipient_id={recipient_id}: {str(e)}")

# ------------------------------------------
# 4. Create DataFrame with Explicit Schema
# ------------------------------------------
schema = StructType([
    StructField("pst_id", IntegerType(), True),
    StructField("recipient_id", IntegerType(), True),
    StructField("scheduled_at", TimestampType(), True),
    StructField("delivered_at", TimestampType(), True),
    StructField("opened_at", TimestampType(), True),
    StructField("clicked_at", TimestampType(), True),
    StructField("replied_at", TimestampType(), True),
    StructField("attachment_opened_at", TimestampType(), True),
    StructField("macro_enabled_at", TimestampType(), True),
    StructField("data_entered_at", TimestampType(), True),
    StructField("qr_code_scanned", TimestampType(), True),
    StructField("reported_at", TimestampType(), True),
    StructField("bounced_at", TimestampType(), True),
    StructField("ip", StringType(), True),
    StructField("ip_location", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("browser_version", StringType(), True),
    StructField("os", StringType(), True),
    StructField("user", StructType([
        StructField("id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("provisioning_guid", StringType(), True),
    ])),
    StructField("template", StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
    ]))
])

# Load raw data without strict schema
detailed_df = spark.createDataFrame(detailed_recipient_data)

# Cast ISO string timestamps to Spark TimestampType
from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

timestamp_cols = [
    "scheduled_at", "delivered_at", "opened_at", "clicked_at", "replied_at",
    "attachment_opened_at", "macro_enabled_at", "data_entered_at",
    "qr_code_scanned", "reported_at", "bounced_at"
]

for col_name in timestamp_cols:
    detailed_df = detailed_df.withColumn(col_name, col(col_name).cast(TimestampType()))


# Flatten user and template fields
detailed_df = detailed_df \
    .withColumn("user_id", col("user.id")) \
    .withColumn("user_first_name", col("user.first_name")) \
    .withColumn("user_last_name", col("user.last_name")) \
    .withColumn("user_email", col("user.email")) \
    .withColumn("template_id", col("template.id")) \
    .withColumn("template_name", col("template.name")) \
    .drop("user", "template")

# ------------------------------------------
# 5. Join with Test Metadata and Save
# ------------------------------------------
final_df = detailed_df.join(test_df.drop("template_name"), on="pst_id", how="inner")

final_df.coalesce(1).write.mode("overwrite").saveAsTable("KnowBe4PhishingTestData")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
