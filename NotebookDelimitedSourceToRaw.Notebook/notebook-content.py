# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import json
from delta.tables import DeltaTable
from datetime import datetime, timezone
from pyspark.sql.functions import lit
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from collections import defaultdict
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

# PARAMETERS CELL ********************

TaskList = ''
GUIDList = ''

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
        source_path = f"{raw_lh_basepath}/Files/{task['SourceWildcardFolderPath']}/{task['SourceWildcardFileName']}"
        dest_path = f"{raw_lh_basepath}/Files/{task['RawStoragePath']}/{task['RawStorageFileName']}"

        raw_df = spark.read.text(source_path)
        # Collect lines
        lines = raw_df.rdd.map(lambda r: r[0])
        # Group lines by file
        files_with_lines = lines.zipWithIndex().map(lambda x: (x[1], x[0]))  # Use index as surrogate to sort
        sorted_lines = files_with_lines.sortByKey().values().collect()

        # Extract header after skiprows
        if len(sorted_lines) > skiprows:
            header = sorted_lines[skiprows].split(delimiter)

            # Get all data rows after header
            data_lines = sorted_lines[skiprows + 1:]

            # Convert to DataFrame
            rdd_data = spark.sparkContext.parallelize(data_lines).map(lambda row: row.split(delimiter))
            df = spark.createDataFrame(rdd_data, schema=header)

            # Write to Parquet
            df.write.mode("overwrite").parquet(dest_path)

            print("All files flattened and written to Parquet.")
        else:
            print("Not enough rows to skip and extract a header.")
    except Exception as e:
        error_message = str(e)[:400]
        print(f"\n ERROR: TaskKey {task['TaskKey']} failed: {error_message}")

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
