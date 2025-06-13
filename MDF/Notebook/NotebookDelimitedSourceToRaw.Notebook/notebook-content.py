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
from delta.tables import DeltaTable
from datetime import datetime, timezone
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from collections import defaultdict
import fnmatch
spark.conf.set("spark.sql.caseSensitive","true")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.analyzer.maxIterations", 1000)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# PARAMETERS CELL ********************

TaskList = ''
GUIDList = ''
RawLhId = ''
WorkspaceId = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# TaskList = "{\"JobAuditKey\":281,\"TaskKey\":5002,\"TaskType\":\"FileTask\",\"SourceName\":\"IndexReturn\",\"SourceType\":\"file\",\"FileType\":\"delimited\",\"SourceWildcardFolderPath\":\"proworkspace/IndexReturn\",\"SourceWildcardFileName\":\"*.txt\",\"SourceDataSet\":\"\",\"PrimaryKeyColumnList\":\"\",\"Delimiter\":\"|\",\"RawLakehouseName\":\"lh_raw\",\"RawStoragePath\":\"IndexReturn\",\"RawStorageFileName\":\"BloombergIndexReturn\",\"ETLWarehouseName\":\"etlControl\",\"BronzeWorkspaceName\":\"Dev - Crimson\",\"BronzeLakehouseName\":\"lh_bronze\",\"BronzeSchemaName\":\"Bronze\",\"BronzeObjectName\":null,\"BronzeObject\":null,\"BronzeLoadMethod\":\"overwrite\",\"WatermarkColumn\":\"ETLLoadDateTime\",\"SinkTableName\":\"BloombergIndexReturn\",\"SinkSchemaName\":\"Bronze\",\"SourceTableName\":\"BloombergIndexReturn\",\"SinkWatermarkColumn\":\"ETLLoadDateTime\",\"SinkLoadMethod\":\"overwrite\",\"IsWatermarkEnabledFlag\":false,\"SourceFullExtractOverrideFlag\":null,\"SkipRows\":13,\"SinkWorkspaceName\":\"Dev - Crimson\"}"
# RawLhId = "920a12cc-7104-4013-a2a3-d7baa57e9e3f"
# WorkspaceId = "33535eb8-4d07-49bc-b3a5-cc91d3aa6ced"
# GUIDList = "[{\"ConfigKey\":\"SPJobAuditStartLock\",\"ConfigValue\":\"0\"},{\"ConfigKey\":\"CuratedLakehouseId\",\"ConfigValue\":\"e9fc4e80-ff69-4d45-bbdd-892592889465\"},{\"ConfigKey\":\"BronzeLakehouseId\",\"ConfigValue\":\"13ef97da-5da2-466d-8c5f-2a70572c6558\"},{\"ConfigKey\":\"Dev - Crimson\",\"ConfigValue\":\"33535eb8-4d07-49bc-b3a5-cc91d3aa6ced\"},{\"ConfigKey\":\"RawLakehouseId\",\"ConfigValue\":\"920a12cc-7104-4013-a2a3-d7baa57e9e3f\"}]"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert JSON String to list
tasklist = [json.loads(TaskList)]
guidlist = json.loads(GUIDList)

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

def process_task(task, raw_lh_basepath):
    try:
        print(f"Processing TaskKey: {task['TaskKey']}. Sink: {task['SinkTableName']}")
        skiprows = task['SkipRows']
        delimiter = task['Delimiter']

        source_folder = f"{raw_lh_basepath}/Files/{task['SourceWildcardFolderPath']}"
        file_pattern = task['SourceWildcardFileName']
        dest_path = f"{raw_lh_basepath}/Files/{task['RawStoragePath']}/{task['RawStorageFileName']}"

        print(f"Source folder: {source_folder}")
        print(f"Destination: {dest_path}")
        print(f"File pattern: {file_pattern}")

        # List all matching files
        all_files = mssparkutils.fs.ls(source_folder)

        matching_files = [f.path for f in all_files if fnmatch.fnmatch(f.name, file_pattern)]

        print(matching_files)

        if not matching_files:
            print("No matching files found.")
            return

        dfs = []

        for file_path in matching_files:
            print(f"\nReading file: {file_path}")
            lines = spark.read.text(file_path).rdd.map(lambda r: r[0]).collect()
            print(lines)

            try:
                header = lines[skiprows].split(delimiter)
                print(header)
            except IndexError:
                print(f"Skipping file {file_path}: Unable to read header line at index {skiprows}.")
                continue

            header = lines[skiprows].split(delimiter)
            data_lines = lines[skiprows + 1:]
            print(data_lines)
            split_rows = [row.split(delimiter) for row in data_lines]
            valid_rows = [r for r in split_rows if len(r) == len(header)]

            if not valid_rows:
                print(f"Skipping file {file_path}: No valid rows match header length {len(header)}.")
                continue

            rdd_data = spark.sparkContext.parallelize(valid_rows)
            df = spark.createDataFrame(rdd_data, schema=header)

            dfs.append(df)
            df.show()

        if not dfs:
            print("No valid DataFrames to process.")
            return

        final_df = dfs[0]
        for df in dfs[1:]:
            final_df = final_df.unionByName(df)

        final_df.write.mode("overwrite").parquet(dest_path)
        print(f"Written {len(dfs)} file(s) to Parquet at: {dest_path}")

    except Exception as e:
        error_message = str(e)[:400]
        print(f"\nERROR: TaskKey {task['TaskKey']} failed: {error_message}")
        raise e

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

loaded_files = []

for task_item in tasklist:
    try:
        # Get workspace and lakehouse keys from task item
        raw_ws_key = task_item.get('SinkWorkspaceName')

        if not raw_ws_key:
            print(f"Task item missing 'SinkWorkspaceName': {task_item}")
            continue

        # Get GUIDs safely
        RawWorkspaceId = get_guid_value(raw_ws_key)
        RawLakehouseId = get_guid_value('RawLakehouseId')

        # Build paths
        raw_lh_basepath = get_basepath(RawWorkspaceId, RawLakehouseId)

        # Process the task
        result = process_task(task_item, raw_lh_basepath)
        loaded_file = result["SourcePath"] if result else None
        loaded_files.append(loaded_file)
        print(f"Processed: {loaded_file}")

    except Exception as e:
        
        print(f"Error processing task: {e}")
        raise Exception

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
