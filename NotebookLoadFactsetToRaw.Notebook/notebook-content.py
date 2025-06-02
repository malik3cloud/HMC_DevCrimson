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

# MARKDOWN ********************

# Overwrite factset files from blob shortcut to Raw storage in raw lakehouse

# CELL ********************

import json
from datetime import datetime, timezone, timedelta
import re


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

fileinfo = ''
watermark = ''
adhoc_date = '' #has to match file naming convention YYYYMMDD
globalconfig = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert JSON String to list
file_info = json.loads(fileinfo)
watermarklist = json.loads(watermark)
config = json.load(globalconfig)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_value_config(key):
    value = next((item['ConfigValue'] for item in guidlist if item['ConfigKey'] == key), None)
    if value is None:
        print(f"Missing value for key: {key}")
    return value

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

from datetime import datetime

return_value = []

def process_task(file_task, watermarklist, adhoc_date=None):
    raw_lh_id = get_value_config('RawLakehouseId')
    workspace_id = get_value_config(file_task['SinkWorkspaceName'])
    raw_lh = get_basepath(workspace_id,raw_lh_id)
    filename_prefix = file_task['SourceWildcardFileName']
    filefolder = file_task['SourceWildcardFolderPath']
    rawpath = file_task['RawStoragePath']
    rawfilename = file_task['RawStorageFileName']
    parquet_output_path = f"{raw_lh}/Files/{rawpath}/{rawfilename}.parquet"


    # Get watermark value for this task (stored as a string in YYYYMMDD format)
    hwm = next((item["HighWatermarkValue"] for item in watermarklist if item["FileTaskKey"] == file_task["FileTaskKey"]), None)
    
    if hwm:
        # Watermark is already a string in YYYYMMDD format, no need to convert
        watermark_str = str(hwm)
    else:
        # If watermark is not found, default to '19900101' (string format)
        watermark_str = '19900101'

    # List all files in folder
    base_path = f"{raw_lh}/Files/{filefolder}"
    all_files = mssparkutils.fs.ls(base_path)

    # If adhoc_date is provided, load only the file for that date
    files_to_read = []
    processed_dates = []
    if adhoc_date and adhoc_date != '':
        # If adhoc_date is provided, filter by that date only
        adhoc_date_str = adhoc_date.strftime("%Y%m%d")
        for file in all_files:
            name = file.name  # e.g., 'mydata_20250510.txt'
            if name.startswith(f"{filename_prefix}_") and name.endswith(".txt"):
                date_str = name[len(filename_prefix) + 1 : -4]  # Extract YYYYMMDD from 'mydata_20250510.txt'
                if date_str == adhoc_date_str:
                    files_to_read.append(file.path)
                    processed_dates.append(date_str)  # Track processed dates
                    break  # Only one file to load, so break the loop
    else:
        # Filter files that have date > watermark
        for file in all_files:
            name = file.name  # e.g., 'mydata_20250510.txt'
            if name.startswith(f"{filename_prefix}_") and name.endswith(".txt"):
                date_str = name[len(filename_prefix) + 1 : -4]  # Extract YYYYMMDD from 'mydata_20250510.txt'
                if date_str > watermark_str:  # Compare as strings (YYYYMMDD format)
                    files_to_read.append(file.path)
                    processed_dates.append(date_str)  # Track processed dates

    # Load and combine data
    if not files_to_read:
        print("No new files to process after watermark or no file for the provided adhoc date.")
        return

    combined_df = spark.read.option("header", "true").option("delimiter", "|").csv(files_to_read)

    # Write to single Parquet file
    combined_df.write.mode("overwrite").parquet(parquet_output_path)

    print(f"Processed {len(files_to_read)} files to {parquet_output_path}")

    # Return Values
    if processed_dates:
         # Get the latest (max) date from processed files (as a string)
        latest_processed_date = max(processed_dates)
        new_watermark = latest_processed_date 
        return_value.append({
            "HighWatermarkValue": new_watermark,
            "TaskKey": file_task['FileTaskKey'],
            "TaskType": "FileTask",
        })
    else:
        print("No files processed")
        return_value.append({
            "HighWatermarkValue": '',
            "TaskKey": file_task['FileTaskKey'],
            "TaskType": "FileTask",
        })
        

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for task_item in file_info:
    process_task(task_item, watermarklist, adhoc_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from notebookutils import mssparkutils

# Return JSON object
mssparkutils.notebook.exit(json.dumps(return_value))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
