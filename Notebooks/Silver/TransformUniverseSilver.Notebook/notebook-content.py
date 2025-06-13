# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "13ef97da-5da2-466d-8c5f-2a70572c6558",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "33535eb8-4d07-49bc-b3a5-cc91d3aa6ced",
# META       "known_lakehouses": [
# META         {
# META           "id": "13ef97da-5da2-466d-8c5f-2a70572c6558"
# META         },
# META         {
# META           "id": "e9fc4e80-ff69-4d45-bbdd-892592889465"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#Setup configurations

spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_curated")
spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_Bronze")

# Load public universe data from bronze layer

df_ispublic = spark.sql(""" SELECT * FROM Bronze.CrimsonXUniverse where OwnerId IN( 'SYSTEMACCOUNT','snevilyj') and IsPublic =1  """)

# Write public Universe data to ih_bronze.Bronze 
lakehousePath = "abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/13ef97da-5da2-466d-8c5f-2a70572c6558"
tableName = "UniversePublic" 

deltaTablePath = f"{lakehousePath}/Tables/Bronze/{tableName}" 
df_ispublic.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(deltaTablePath)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark
from datetime import datetime, date

# -----------
# Set up necessary configurations
spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_curated")
spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_Bronze")

df_final =[]
dfuniverse = spark.sql("SELECT upper(UniverseId) as UniverseId,FilterExpression FROM Bronze.UniversePublic")
#display(dfuniverse) 
dfvwUniverse =  spark.sql("SELECT upper(UniverseId) as UniverseId,UniverseName,UniverseDescription FROM Bronze.UniversePublic")
#display(dfvwUniverse)

# # Filter out rows with null or empty FilterExpression
dfuniverse = dfuniverse.filter(dfuniverse.FilterExpression.isNotNull() & (dfuniverse.FilterExpression != ""))

# # Iterate over each universe and collect FundIds for the universe
for row in dfuniverse.collect():
    universe_id = row["UniverseId"]
    filter_expression = row["FilterExpression"]
    filter_expression = filter_expression.replace("vwUniverseFunds","Bronze.vwUniverseFunds")

    if filter_expression and len(filter_expression.strip()) > 0:
        query = f"SELECT distinct upper(FundId) as FundId FROM Bronze.vwUniverseFunds WHERE {filter_expression}"
        #print(query)
        dfUniverseFunds_filtered = spark.sql(query)
        for fund_id in dfUniverseFunds_filtered.select("FundId").collect():
            df_final.append((universe_id, fund_id["FundId"]))

schema = ["UniverseId", "FundId"]
df_table = spark.createDataFrame(df_final, schema=schema)
#display(df_table)

# #Write Universefunds and Vwuniversefunds to Silver
lakehousePath = "abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/e9fc4e80-ff69-4d45-bbdd-892592889465"
tableName = "UniverseFunds" # In_TableToProcess # "Entity"
table_Name = "Universe"
deltaTablePath = f"{lakehousePath}/Tables/Silver/{tableName}" 
vwTablePath = f"{lakehousePath}/Tables/Silver/{table_Name}" 


# #Write the dataframe to the Delta table
df_table.write.format("delta").mode("overwrite").save(deltaTablePath)
dfvwUniverse.write.format("delta").mode("overwrite").save(vwTablePath)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
