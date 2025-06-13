# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e9fc4e80-ff69-4d45-bbdd-892592889465",
# META       "default_lakehouse_name": "lh_curated",
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

#Create  Fund data into Silver zone
# ----------- Set the namespace for the Lakehouse
spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_curated")
spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_Bronze")

spark.sql("SET spark.sql.caseSensitive = TRUE")

from datetime import datetime,date
from delta.tables import DeltaTable
import pyspark.sql.functions as F

classification_df = spark.table("Silver.Classification").select("FundId", "ClassificationId")

df_crimsonx_fund = spark.table("lh_bronze.Bronze.CrimsonXFund")

final_df = classification_df.join(df_crimsonx_fund,on = "FundId",how ="inner")

final_df = final_df.select("FundId","FundName","EntityId","ClassificationId","FundStructureId","InvestmentFormId","VintageYear")
#print(final_df.columns)

final_df = final_df.withColumnRenamed("FundStructureId","FundStructure")\
.withColumnRenamed("InvestmentFormId","InvestmentForm")
                    
lakehousePath = "abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/e9fc4e80-ff69-4d45-bbdd-892592889465"
tableName = "Fund" 

deltaTablePath = f"{lakehousePath}/Tables/Silver/{tableName}" 
final_df.write.format("delta").mode("overwrite").save(deltaTablePath)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Load Firm to Silver
df_crimsonx_firm = spark.table("lh_bronze.Bronze.CrimsonXFirm")
df_crimsonx_firm = df_crimsonx_firm.select("FirmId")

#write firm table to silver
lakehousePath = "abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/e9fc4e80-ff69-4d45-bbdd-892592889465"
tableName = "Firm" 

deltaTablePath = f"{lakehousePath}/Tables/Silver/{tableName}" 
df_crimsonx_firm.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(deltaTablePath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Load Entity to Silver
df_crimsonx_entity = spark.table("lh_bronze.Bronze.CrimsonXEntity")
df_crimsonx_entity = df_crimsonx_entity.select("EntityId","EntityName","FirmId","IsActive","BusinessOwnerHMCUserId","BackupBusinessOwnerHMCUserId","SecondaryBackupBusinessOwnerHMCUserId","AssociateHMCUserId")
df_crimsonx_entity = df_crimsonx_entity.withColumnRenamed("IsActive","EntityStatus")\
.withColumnRenamed("BackupBusinessOwnerHMCUserId","SecondaryOwner")\
.withColumnRenamed("SecondaryBackupBusinessOwnerHMCUserId","TertiaryOwner")\
.withColumnRenamed("AssociateHMCUserId","Associate")

#write Entity table to silver
lakehousePath = "abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/e9fc4e80-ff69-4d45-bbdd-892592889465"
tableName = "Entity" 

deltaTablePath = f"{lakehousePath}/Tables/Silver/{tableName}" 
df_crimsonx_entity.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(deltaTablePath)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# spark.conf.set("spark.sql.catalog.spark_catalog.defaultNamespace", "lh_curated")
# spark.sql("""drop table Silver.Fund""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
