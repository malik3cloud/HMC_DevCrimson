# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Use Microsoft Entra Connector configured in Fabric

# CELL ********************


connection_name = "Microsoft Entra Connector"




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Endpoint to get users from Microsoft Graph

# CELL ********************


api_path = "/v1.0/users?$top=999"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# MARKDOWN ********************

# # Create Spark session

# CELL ********************


from pyspark.sql import SparkSession
from notebookutils.connection import get_connection

spark = SparkSession.builder.getOrCreate()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Get access token from Fabric connector


# MARKDOWN ********************


# CELL ********************


conn = get_connection(connection_name)
access_token = conn.get_token()  # Uses built-in Fabric token management


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Make request to Graph API

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



import requests

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

graph_url = f"https://graph.microsoft.com{api_path}"
response = requests.get(graph_url, headers=headers)

if response.status_code != 200:
    raise Exception(f"Graph API request failed: {response.status_code} - {response.text}")

users_data = response.json().get("value", [])



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Define schema (add fields as required)

# CELL ********************


from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("id", StringType(), True),
    StructField("displayName", StringType(), True),
    StructField("mail", StringType(), True),
    StructField("userPrincipalName", StringType(), True),
    StructField("jobTitle", StringType(), True)
])



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Transform JSON to rows

# CELL ********************


rows = [
    (u.get("id"), u.get("displayName"), u.get("mail"), u.get("userPrincipalName"), u.get("jobTitle"))
    for u in users_data
]




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Create DataFrame

# CELL ********************


df = spark.createDataFrame(rows, schema)
df.show(truncate=False)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Save to Lakehouse 

# CELL ********************


# df.write.mode("overwrite").format("delta").save("Tables/EntraUsers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
