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

# ----------- Set the namespace for the Lakehouse
spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_curated")
spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_Bronze")
# -----------

# SQL query
df  = spark.sql("""
SELECT DISTINCT f.FundId, f.FundClassificationId as ClassificationId
       , c.Description AS AssetClass
       , c1.Description AS Strategy
       , c2.Description AS SubStrategy
FROM (
    SELECT f.FundId
           , CASE WHEN fcm.HMCObjectId IS NOT NULL THEN fcm.Level1ClassificationId ELSE ecm.Level1ClassificationId END AS Level1ClassificationId
           , CASE WHEN fcm.HMCObjectId IS NOT NULL THEN fcm.Level2ClassificationId ELSE ecm.Level2ClassificationId END AS Level2ClassificationId
           , CASE WHEN fcm.HMCObjectId IS NOT NULL THEN fcm.Level3ClassificationId ELSE ecm.Level3ClassificationId END AS Level3ClassificationId
           , CASE WHEN fcm.HMCObjectId IS NOT NULL THEN coalesce(fcm.Level3ClassificationId, coalesce(fcm.Level2ClassificationId, fcm.Level1ClassificationId))
                  ELSE coalesce(ecm.Level3ClassificationId, coalesce(ecm.Level2ClassificationId, ecm.Level1ClassificationId)) END AS FundClassificationId
    FROM Bronze.CrimsonXFund f
         JOIN Bronze.CrimsonXClassificationMap AS ecm
           ON f.EntityId = ecm.HMCObjectId AND ecm.EndDate IS NULL
         LEFT JOIN Bronze.CrimsonXClassificationMap AS fcm
           ON f.FundId = fcm.HMCObjectId AND fcm.EndDate IS NULL
) f
JOIN lh_Bronze.Bronze.CrimsonXClassification AS c
  ON c.ClassificationId = f.Level1ClassificationId
LEFT JOIN lh_Bronze.Bronze.CrimsonXClassification AS c1
  ON c1.ClassificationId = f.Level2ClassificationId
LEFT JOIN lh_Bronze.Bronze.CrimsonXClassification AS c2
  ON c2.ClassificationId = f.Level3ClassificationId
""")

lakehousePath = "abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/e9fc4e80-ff69-4d45-bbdd-892592889465"
tableName = "Classification" 

deltaTablePath = f"{lakehousePath}/Tables/Silver/{tableName}" 

df.write.format("delta").mode("overwrite").save(deltaTablePath)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#----------- Set the namespace for the Lakehouse
# spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_curated")
# #-----------
# spark.sql("SET spark.sql.caseSensitive = TRUE")
# spark.sql("drop table  lh_curated.Silver.Classification")
# spark.sql("Create table lh_curated.Silver.Classification(FundId VARCHAR(5000),ClassificationId INTEGER,AssetClass VARCHAR(5000),Strategy VARCHAR(5000),SubStrategy VARCHAR(5000)) ")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
