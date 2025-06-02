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

# PARAMETERS CELL ********************

# --------------------------------------
# CONFIGURATION
# --------------------------------------
bronze_lakehouse_name = "lh_bronze"
curated_lakehouse_name = "lh_curated"
target_table_name = "Silver.02Test"  # Output in curated lakehouse



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



from datetime import datetime
from pyspark.sql.functions import lit
from notebookutils import mssparkutils
from zoneinfo import ZoneInfo



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# GET CURRENT EST DATETIME

etlloadDateTime = datetime.now(ZoneInfo("America/New_York"))
print(etlloadDateTime) 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sql_query = f"""
    SELECT 
    Date as ClosingDate
    , 'XXXX2' as Index
    , Open AS DayOpen
    , High AS DayHigh 
    , Low AS DayLow
    , Price AS DayClose
    , Vol AS DayVolume
    FROM {bronze_lakehouse_name}.Bronze.02Test
    WHERE 1=1  -- Optional filter
"""


display(sql_query)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# --------------------------------------
# STEP 1: Execute SQL query to fetch source data
# --------------------------------------
df = spark.sql(sql_query)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --------------------------------------
# STEP 2: Add ETL metadata columns
# --------------------------------------

df_enriched = df.withColumn("ETLLoadDatetime", lit(etlloadDateTime)) 

#display(df_enriched)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --------------------------------------
# STEP 3: Write enriched data to lh_curated
# --------------------------------------

df_enriched.write \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{curated_lakehouse_name}.{target_table_name}")


print("ETL process completed successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
