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

import json
from pyspark.sql.functions import col, hex
import json
from delta.tables import DeltaTable
from datetime import datetime, timezone
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from collections import defaultdict

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

TaskList = ''
GlobalConfig = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# # This cell is generated from runtime parameters. Learn more: https://go.microsoft.com/fwlink/?linkid=2161015
# GlobalConfig = "[{\"ConfigKey\":\"SPJobAuditStartLock\",\"ConfigValue\":\"0\"},{\"ConfigKey\":\"BronzeLakehouseId\",\"ConfigValue\":\"13ef97da-5da2-466d-8c5f-2a70572c6558\"},{\"ConfigKey\":\"Dev - Crimson\",\"ConfigValue\":\"33535eb8-4d07-49bc-b3a5-cc91d3aa6ced\"},{\"ConfigKey\":\"RawLakehouseId\",\"ConfigValue\":\"920a12cc-7104-4013-a2a3-d7baa57e9e3f\"}]"
# TaskList = "[{\"TaskKey\":8,\"TaskRunOrderNbr\":1,\"JobKey\":5,\"ParentSourceName\":\"Database\",\"SourceName\":\"Database\",\"SourceType\":\"sqlserver\",\"TaskName\":\"FundTradeApproval\",\"TaskType\":\"DatabaseTask\",\"SinkLoadMethod\":\"overwrite\",\"PrimaryKeyColumnList\":null,\"IsWatermarkEnabledFlag\":false,\"SourceWatermarkColumn\":null,\"SinkWatermarkColumn\":\"ETLLoadDateTime\",\"SourceWorkspaceName\":\"Dev - Crimson\",\"SourceLakehouseName\":null,\"SourceWarehouseName\":null,\"SourceDatabaseName\":\"SQLServer_CrimsonX\",\"SourceSchemaName\":\"dbo\",\"SourceTableName\":\"FundTradeApproval\",\"SinkWorkspaceName\":\"Dev - Crimosn\",\"SinkWorkspaceId\":null,\"SinkLakehouseName\":\"lh_bronze\",\"SinkLakehouseId\":null,\"SinkWarehouseName\":null,\"SinkSchemaName\":null,\"SinkTableName\":\"FundTradeStatus\",\"SourceFlatfileConnectionSettings\":null,\"NotebookKey\":null,\"RawStoragePath\":\"rawtest\",\"RawStorageFileName\":\"FundTradeApproval\",\"ArchiveOriginalFilesFlag\":true,\"ArchiveStoragePath\":null,\"ArchiveStorageFileName\":null,\"IsActiveFlag\":true,\"SourceExtractionMethod\":null,\"OverrideQuery\":null,\"SourceWhereClause\":null}]"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert JSON String to list
tasklist = json.loads(TaskList)
guidlist = json.loads(GlobalConfig)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_basepath(
    workspace_id, 
    lakehouse_id
) -> str:
    lh_basepath = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}"
    return lh_basepath

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, hex
from notebookutils import mssparkutils

def process_task(task):
    source_file = f"{raw_lh_basepath}/Files/{task['RawStoragePath']}/{task['RawStorageFileName']}.parquet"
    print(f"Reading from: {source_file}")

    target_path = f"{raw_lh_basepath}/Files/{task['RawStoragePath']}/{task['RawStorageFileName']}"

    df = spark.read.parquet(source_file)


    binary_columns = [f.name for f in df.schema.fields if f.dataType.simpleString() == "binary"]
    print("Binary columns detected:", binary_columns)

    for col_name in binary_columns:
        df = df.withColumn(col_name, hex(col(col_name)))

    df.show(5, truncate=False)
    df.write.mode("overwrite").parquet(target_path)

    print(f"Converted Parquet saved to {target_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_guid_value(key):
    value = next((item['ConfigValue'] for item in guidlist if item['ConfigKey'] == key), None)
    if value is None:
        print(f"Missing GUID for key: {key}")
    return value


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for task_item in tasklist:
    try:
        # Get workspace and lakehouse keys from task item
        bronze_ws_key = task_item.get('SourceWorkspaceName')
        raw_ws_key = task_item.get('SourceWorkspaceName')  # Assuming same key used, correct if different

        if not bronze_ws_key:
            print(f"Task item missing 'SourceWorkspaceName': {task_item}")
            continue

        # Get GUIDs safely
        BronzeWorkspaceId = get_guid_value(bronze_ws_key)
        BronzeLakehouseId = get_guid_value('BronzeLakehouseId')
        RawWorkspaceId = get_guid_value(raw_ws_key)
        RawLakehouseId = get_guid_value('RawLakehouseId')

        # Skip if any required ID is missing
        if not all([BronzeWorkspaceId, BronzeLakehouseId, RawWorkspaceId, RawLakehouseId]):
            print("Skipping due to missing required GUID(s)")
            raise Exception

        # Build paths
        bronze_lh_basepath = get_basepath(BronzeWorkspaceId, BronzeLakehouseId)
        raw_lh_basepath = get_basepath(RawWorkspaceId, RawLakehouseId)
        # Process the task
        result = process_task(task_item)

    except Exception as e:
        
        print(f"Error processing task: {e}")
        raise Exception
    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
