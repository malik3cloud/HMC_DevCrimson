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
# META           "id": "e9fc4e80-ff69-4d45-bbdd-892592889465"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ---------------------------------------------------------------------------
# | Scenario                     | Action                                   |
# |------------------------------|------------------------------------------|
# | First-time write             |  Full load (overwrite + mergeSchema)     |
# | New columns detected         |  Schema evolution (append + mergeSchema) |
# | Column data type change      |  MUST BE MANUALLY CASTED BEFORE WRITE    |
# | Rows exist but changed       |  MERGE updates changed rows              |
# | New rows added               |  MERGE inserts missing rows              |
# | All columns are primary keys |  Only updates HWM if data changed        |
# ---------------------------------------------------------------------------

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

# CELL ********************

# This cell is generated from runtime parameters. Learn more: https://go.microsoft.com/fwlink/?linkid=2161015
TaskList = "[{\"TaskKey\":29,\"TaskRunOrderNbr\":1,\"JobKey\":2,\"ParentSourceName\":\"Database\",\"SourceName\":\"Bronze\",\"SourceType\":\"Lakehouse\",\"TaskName\":\"CrimsonXIndexRegionExposure\",\"TaskType\":\"BronzeToSilver\",\"SinkLoadMethod\":\"overwrite\",\"PrimaryKeyColumnList\":\"NULL\",\"IsWatermarkEnabledFlag\":true,\"SourceWatermarkColumn\":\"ETLLoadDateTime\",\"SinkWatermarkColumn\":\"ETLLoadDateTime\",\"SourceWorkspaceName\":\"Dev - Crimson\",\"SourceLakehouseName\":\"lh_bronze\",\"SourceWarehouseName\":\"NULL\",\"SourceDatabaseName\":\"NULL\",\"SourceSchemaName\":\"Bronze\",\"SourceTableName\":\"CrimsonXIndexRegionExposure\",\"SinkWorkspaceName\":\"Dev - Crimson\",\"SinkWorkspaceId\":\"NULL\",\"SinkLakehouseName\":\"lh_curated\",\"SinkLakehouseId\":\"NULL\",\"SinkWarehouseName\":\"NULL\",\"SinkSchemaName\":\"Silver\",\"SinkTableName\":\"CrimsonXIndexRegionExposure\",\"SourceFlatfileConnectionSettings\":\"NULL\",\"NotebookKey\":0,\"RawStoragePath\":\"NULL\",\"RawStorageFileName\":\"NULL\",\"ArchiveOriginalFilesFlag\":true,\"ArchiveStoragePath\":\"NULL\",\"ArchiveStorageFileName\":\"NULL\",\"IsActiveFlag\":true,\"SourceExtractionMethod\":\"NULL\",\"OverrideQuery\":\"NULL\",\"SourceWhereClause\":\"NULL\"}]"
WatermarkList = "[{\"TaskKey\":29,\"TaskRunOrderNbr\":1,\"JobKey\":2,\"ParentSourceName\":\"Database\",\"SourceName\":\"Bronze\",\"SourceType\":\"Lakehouse\",\"TaskName\":\"CrimsonXIndexRegionExposure\",\"TaskType\":\"BronzeToSilver\",\"SinkLoadMethod\":\"overwrite\",\"PrimaryKeyColumnList\":\"NULL\",\"IsWatermarkEnabledFlag\":true,\"SourceWatermarkColumn\":\"ETLLoadDateTime\",\"SinkWatermarkColumn\":\"ETLLoadDateTime\",\"SourceWorkspaceName\":\"Dev - Crimson\",\"SourceLakehouseName\":\"lh_bronze\",\"SourceWarehouseName\":\"NULL\",\"SourceDatabaseName\":\"NULL\",\"SourceSchemaName\":\"Bronze\",\"SourceTableName\":\"CrimsonXIndexRegionExposure\",\"SinkWorkspaceName\":\"Dev - Crimson\",\"SinkWorkspaceId\":\"NULL\",\"SinkLakehouseName\":\"lh_curated\",\"SinkLakehouseId\":\"NULL\",\"SinkWarehouseName\":\"NULL\",\"SinkSchemaName\":\"Silver\",\"SinkTableName\":\"CrimsonXIndexRegionExposure\",\"SourceFlatfileConnectionSettings\":\"NULL\",\"NotebookKey\":0,\"RawStoragePath\":\"NULL\",\"RawStorageFileName\":\"NULL\",\"ArchiveOriginalFilesFlag\":true,\"ArchiveStoragePath\":\"NULL\",\"ArchiveStorageFileName\":\"NULL\",\"IsActiveFlag\":true,\"SourceExtractionMethod\":\"NULL\",\"OverrideQuery\":\"NULL\",\"SourceWhereClause\":\"NULL\"}]"
GUIDList = None
BronzeLhId = "13ef97da-5da2-466d-8c5f-2a70572c6558"
CuratedLhId = "e9fc4e80-ff69-4d45-bbdd-892592889465"
WorkspaceId = "33535eb8-4d07-49bc-b3a5-cc91d3aa6ced"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

TaskList = ''
WatermarkList = ''
# GUIDList = ''
BronzeLhId = ''
CuratedLhId = ''
WorkspaceId = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert JSON String to list
tasklist = json.loads(TaskList)
watermarklist = json.loads(WatermarkList)
# guidlist = json.loads(GUIDList)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set watermark
etlloadtime = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
print(etlloadtime) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Derive absolute basepath from parameters

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

# MARKDOWN ********************

# ##### Clean column headers to save to Delta Table

# CELL ********************

import re

def sanitize_column_names_with_mapping(df):
    rename_map = {}
    for col in df.columns:
        sanitized = col.replace("%", "Percentage")
        sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", sanitized)
        sanitized = re.sub(r"_+", "_", sanitized).strip("_")
        
        if sanitized != col:
            df = df.withColumnRenamed(col, sanitized)
            rename_map[sanitized] = col  # store reverse mapping for restoring, not used in this code but for future development

    if rename_map:
        print(f"[i] Column rename mapping: {rename_map}")
    return df, rename_map


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def restore_original_column_names(df, rename_map):
    for sanitized, original in rename_map.items():
        df = df.withColumnRenamed(sanitized, original)
    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Code to make sure only one HWM column exists before merge

# CELL ********************

from pyspark.sql.functions import col, coalesce

def unify_hwm_column(df, hwm_column):
    # Identify all columns with HWM name or variants like etlloaddatetime_1
    hwm_candidates = [c for c in df.columns if c == hwm_column or c.startswith(f"{hwm_column}_")]

    if len(hwm_candidates) > 1:
        print(f"Multiple HWM columns detected: {hwm_candidates}. Unifying into one '{hwm_column}'.")

        # Create unified column using coalesce (first non-null wins)
        df = df.withColumn(hwm_column, coalesce(*[col(c) for c in hwm_candidates]))

        # Drop all other variants except the unified one
        cols_to_drop = [c for c in hwm_candidates if c != hwm_column]
        df = df.drop(*cols_to_drop)

    return df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import fs
from delta.tables import DeltaTable
# Initialize Spark
spark = SparkSession.builder.getOrCreate()

return_value = []

def process_task(task, etlloadtime, watermarklist):
    try:
        print(f"Processing TaskKey: {task['TaskKey']}. Sink: {task['SinkTableName']}")

        # Define Parameters
        source_path = f"{bronze_lh_basepath}/Tables/Bronze/{task['SourceTableName']}"
        print(f"Reading: {source_path}")
        target_table = task['SinkTableName']
        target_path = f"{silver_lh_basepath}/Tables/Silver/{target_table}"
        hwm_column = task['SinkWatermarkColumn']
        primary_column = task['PrimaryKeyColumnList']
        method = task['SinkLoadMethod']
        TaskAuditKey = 1
        # hwm_value = next((item["HighWatermarkValue"] for item in watermarklist if item["TaskKey"] == task["TaskKey"]), None)

        # Determine primary keys
        if not primary_column or str(primary_column).strip().upper() == "NULL":
            df_temp = spark.read.format("delta").load(source_path)
            # Use all columns except the watermark column as primary keys
            primary_keys = [col for col in df_temp.columns if col != hwm_column]
            df_source = df_temp.dropDuplicates(primary_keys)
        else:
            primary_keys = [col.strip() for col in primary_column.split(",")]
            df_source = spark.read.format("delta").load(source_path).dropDuplicates(primary_keys)


        # Read Parquet source and clean
        df_source = spark.read.format("delta").load(source_path).dropDuplicates(primary_keys)
        print(f"[debug] Columns after reading and dedup: {df_source.columns}")
        df_source, rename_map = sanitize_column_names_with_mapping(df_source)
        print(f"[debug] Columns after sanitizing: {df_source.columns}")
        df_source = unify_hwm_column(df_source, hwm_column)
        print(f"[debug] Columns after unifying: {df_source.columns}")
        df_source.createOrReplaceTempView("df_source_view")

        # Delta Load
        # if task['IsWatermarkEnabled'] == 1:
        #     watermark_col = task['SourceWatermarkColumn']
        #     if watermark_col not in df_source.columns:
        #         raise Exception(f"Watermark column '{watermark_col}' not found in DataFrame.")
        #     # Apply watermark filter
        #     df_source = df_source.filter(f"{watermark_col} > '{hwm_value}'")



        # Skip processing if source table is empty
        if df_source.rdd.isEmpty():
            print(f"\n Source table {source_path} is empty. Skipping TaskKey: {task['TaskKey']}.")
            RowsRead = 0
            RowsInserted = 0
            RowsUpdated = 0
            RowsDeleted = 0
            return {
                "SourcePath": source_path,
                "RowsRead": RowsRead,
                "RowsInserted": RowsInserted,
                "RowsUpdated": RowsUpdated,
                "RowsDeleted": RowsDeleted
            }
            

        # Check if target table exists
        table_exists = True
        try:
            df_target = spark.read.format("delta").load(target_path)
        except AnalysisException:
            table_exists = False

        RowsRead = df_source.count()  # Count total rows in source

        

        if not table_exists:
            print(f"\nTarget table does not exist. Creating it at: {target_path}")
            if hwm_column not in df_source.columns:
                try:
                    df_source = df_source.withColumn(hwm_column, lit(etlloadtime))
                except Exception as e:
                    print(f"[ERROR] Failed to add watermark column '{hwm_column}': {e}")
                    raise
            df_source.write.format("delta").mode("overwrite").save(target_path)
            return

        elif method == 'overwrite':
            print(f"\nOverwriting existing table at: {target_path}")
            if hwm_column not in df_source.columns:
                try:
                    df_source = df_source.withColumn(hwm_column, lit(etlloadtime))
                except Exception as e:
                    print(f"[ERROR] Failed to add watermark column '{hwm_column}': {e}")
                    raise
            df_source.write.format("delta").mode("overwrite").save(target_path)
            return

        elif method == 'append':
            print(f"\nAppending to existing table at: {target_path}")
            if hwm_column not in df_source.columns:
                try:
                    df_source = df_source.withColumn(hwm_column, lit(etlloadtime))
                except Exception as e:
                    print(f"[ERROR] Failed to add watermark column '{hwm_column}': {e}")
                    raise
            df_source.write.format("delta").mode("append").save(target_path)
            return
        
        else:
            print(f"Method is not overwrite or append, '{method}' — performing merge")

        # Get schema information
        df_source_view_df = spark.table("df_source_view")
        schema_target = {field.name: field.dataType for field in df_target.schema if field.name != hwm_column}
        schema_source = {field.name: field.dataType for field in df_source_view_df.schema if field.name != hwm_column}

        if schema_target != schema_source:
            print(f"Schema change detected on {task['SourceTableName']}")

            # Only add the watermark column if it doesn't already exist
            if hwm_column not in df_source_view_df.columns:
                df_source = df_source_view_df.withColumn(hwm_column, lit(etlloadtime))
            else:
                print(f"Watermark column '{hwm_column}' already exists in source. Skipping addition.")
                df_source = df_source_view_df

            df_source.write.format("delta").mode("append").option("mergeSchema", "true").save(target_path)

            RowsInserted = df_source.count()
            RowsUpdated = 0
            RowsDeleted = 0
            print(f"\n [✓] COMPLETE MergeSchema: TaskKey {task['SinkTableName']}")
            return {
                "SourcePath": source_path,
                "RowsRead": RowsRead,
                "RowsInserted": RowsInserted,
                "RowsUpdated": RowsUpdated,
                "RowsDeleted": RowsDeleted
            }


        # Perform Merge
        print(f"Performing merge on {task['SinkTableName']}")
        print(f"[debug] Columns after checking: {df_source_view_df.columns}")
        # Identify non-primary-key columns (excluding HWM)
        columns = df_source_view_df.columns
        print(f"[debug] Columns after alignment: {df_source_view_df.columns}")

        change_condition = " OR ".join([f"target.{col} IS DISTINCT FROM source.{col}" for col in columns if col not in primary_keys and col != hwm_column])
        print(hwm_column)
        columns_excl_hwm = [col for col in columns if col != hwm_column]
        # Check if all columns are primary keys by comparing with the primary_column list
        if len(primary_keys) == len(columns_excl_hwm):
            # All columns are primary keys, meaning only inserts should happen unless data changes.
            print(f"All columns are primary keys. Ensuring valid update conditions.")

            merge_query = f"""
            MERGE INTO Silver.{target_table} AS target
            USING (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join(primary_keys)} ORDER BY rand()) AS rn
                    FROM df_source_view
                ) AS subquery
                WHERE rn = 1
            ) AS source
            ON ({' AND '.join([f"target.{col} = source.{col}" for col in primary_keys])})
            """

            if change_condition:  # Only include the update condition if there are non-PK columns to compare, this part of the code is for fail-safe. If all columns are PK here, this will not be added
                merge_query += f"""
                WHEN MATCHED
                AND ({change_condition})
                THEN
                    UPDATE SET target.{hwm_column} = '{etlloadtime}'
                """

            insert_columns = columns.copy()
            if hwm_column not in insert_columns:
                insert_columns.append(hwm_column)

            insert_values = [f"source.{col}" for col in columns]
            if hwm_column not in columns:
                insert_values.append(f"'{etlloadtime}'")
            else:
                insert_values.append(f"source.{hwm_column}")

            merge_query += f"""
                WHEN NOT MATCHED THEN
                    INSERT ({', '.join(insert_columns)})
                    VALUES ({', '.join(insert_values)});
                """
            print(merge_query)


        else:
            # Regular merge logic (non-primary-key columns can be updated)
            merge_query = f"""
            MERGE INTO Silver.{target_table} AS target
            USING (
                SELECT * FROM (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY {', '.join(primary_keys)} ORDER BY rand()) AS rn
                    FROM df_source_view
                ) AS subquery
                WHERE rn = 1
            ) AS source
            ON ({' AND '.join([f"target.{col} = source.{col}" for col in primary_keys])})
            WHEN MATCHED
            AND ({change_condition})
            THEN
                UPDATE SET {', '.join([f"target.{col} = source.{col}" for col in columns if col != hwm_column])},
                        target.{hwm_column} = '{etlloadtime}'
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(columns)}, {hwm_column})
                VALUES ({', '.join([f'source.{col}' for col in columns])}, '{etlloadtime}'); 
            """

        spark.sql(merge_query)


        delta_table = DeltaTable.forPath(spark, target_path)
        # Load as DataFrame, not DeltaTable
        df_target = spark.read.format("delta").load(target_path)

        # Apply reverse column renaming to DataFrame
        df_restored = restore_original_column_names(df_target, rename_map)

        # Get the latest commit history
        history_df_audit = delta_table.history(1)  # Get last operation only

        # Extract operation metrics
        operation_metrics = history_df_audit.select("operationMetrics").collect()[0][0]
        RowsInserted = int(operation_metrics.get("numOutputRows", 0))  # Rows written
        RowsUpdated = int(operation_metrics.get("numUpdatedRows", 0))  # Updated rows
        RowsDeleted = int(operation_metrics.get("numDeletedRows", 0))  # Deleted rows

        
        # Overwrite the cleaned DataFrame
        print("restore")
        # df_restored.write.format("delta").mode("overwrite").save(target_path)

        print(f"\n [✓] COMPLETE Merge: TaskKey {task['SinkTableName']}")
        return {
            "SourcePath": source_path,
            "RowsRead": RowsRead,
            "RowsInserted": RowsInserted,
            "RowsUpdated": RowsUpdated,
            "RowsDeleted": RowsDeleted
        }

    except Exception as e:
        error_message = str(e)[:400]
        print(f"\n ERROR: TaskKey {task['TaskKey']} failed: {error_message}")
        RowsRead = 0
        RowsInserted = 0
        RowsUpdated = 0
        RowsDeleted = 0
        return {
            "SourcePath": None,
            "RowsRead": RowsRead,
            "RowsInserted": RowsInserted,
            "RowsUpdated": RowsUpdated,
            "RowsDeleted": RowsDeleted
        }
        raise Exception



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

        # Skip if any required ID is missing
        if not all([BronzeLhId, CuratedLhId, WorkspaceId]):
            print("Skipping due to missing required GUID(s)")
            raise Exception
            
        # Build paths
        bronze_lh_basepath = get_basepath(WorkspaceId, BronzeLhId)
        silver_lh_basepath = get_basepath(WorkspaceId, CuratedLhId)

        # Process the task
        result = process_task(task_item, etlloadtime, watermarklist)
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

# CELL ********************

# from datetime import datetime
# import re
# from notebookutils import mssparkutils  # Import Fabric-specific file system utilities

# # Archive path (already created earlier in the code)
# archive_path = f"{raw_lh_basepath}/Files/Archive"
# print(archive_path)

# def move_to_archive(file_path):
#     file_path = file_path.rstrip('/')  # Remove trailing slash
#     print(f"Processed file path: {file_path}")
    
#     # Get current date (year, month, day)
#     current_date = datetime.now()
#     year = current_date.year
#     month = current_date.strftime('%m')  # Zero-padded month
#     day = current_date.strftime('%d')   # Zero-padded day

#     # Extract folder name like "01Test" from the input file path
#     match = re.search(r'/Files/([^/]+)(?:/|$)', file_path)

#     print(f"Regex match result: {match}")
    
#     if not match:
#         print(" Unable to extract source folder (e.g., 01Test) from path.")
#         return
    
#     table_folder = match.group(1)
#     print(f"Extracted folder: {table_folder}")

#     # Construct full archive destination path
#     archive_dir = f"{archive_path}/{year}/{month}/{day}"
#     file_name = file_path.split('/')[-1]  # Extract the file name from the path
#     destination_path = f"{archive_dir}"

#     print(f"Moving to: {destination_path}")

#     try:
#         # Ensure the destination directory exists in Fabric (ADLS/OneLake)
#         # In Fabric, use mssparkutils.fs.mkdirs for directory creation (works with ADLS/OneLake)
#         mssparkutils.fs.mkdirs(archive_dir)

#         # Use mssparkutils.fs.mv() to move the file in Fabric
#         mssparkutils.fs.mv(file_path, destination_path)

#         print(f"File moved successfully to: {destination_path}")
#     except Exception as e:
#         print(f"Error occurred while moving file: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import fs 
for item_file in loaded_files:
    print(item_file)
    if item_file is not None:
        move_to_archive(item_file)
    else:
        print("Skipped: item_file is None.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

audit_result = {
    "RowsCopied": result['RowsRead'],
    "RowsInserted": result['RowsInserted'],
    "RowsUpdated": result['RowsUpdated'],
    "RowsDeleted": result['RowsDeleted']
}
notebookutils.notebook.exit(str(audit_result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# from mssparkutils.fs import cp, exists

# def copy_abfss_file(source_path: str, destination_path: str):
#     """
#     Copies a file from one ABFSS path to another.
    
#     Parameters:
#         source_path (str): The source ABFSS path (e.g., "abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<file_path>").
#         destination_path (str): The destination ABFSS path (e.g., "abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<new_file_path>").
#     """
    
#     # Check if the source file exists
#     if exists(source_path):
#         # Perform the copy operation
#         cp(source_path, destination_path)
#         print(f"[✓] File copied from {source_path} to {destination_path}")
#     else:
#         print(f"[❗] Source file does not exist: {source_path}")

# # Example usage
# source_path = "abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/2c080a33-8d2c-4c79-9dec-e1e74d700242/Files/01Test"
# destination_path = "abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/bd682b0f-5772-451c-9de9-3c412bddbd53/Files/01Test"

# copy_abfss_file(source_path, destination_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
