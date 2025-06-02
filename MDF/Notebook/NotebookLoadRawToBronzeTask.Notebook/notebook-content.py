# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

# This cell is generated from runtime parameters. Learn more: https://go.microsoft.com/fwlink/?linkid=2161015
TaskKey = 9
TaskList = "{\"JobAuditKey\":154,\"TaskKey\":9,\"TaskType\":\"DatabaseTask\",\"SourceName\":\"Database\",\"SourceType\":\"sqlserver\",\"SourceDatabaseName\":\"SQLServer CrimsonX - Prod\",\"SourceSchemaName\":\"dbo\",\"SourceTableName\":\"FundTradeType\",\"PrimaryKeyColumnList\":\"\",\"RawStoragePath\":\"CrimsonX\",\"RawStorageFileName\":\"FundTradeType\",\"ArchiveOriginalFilesFlag\":true,\"ArchiveStoragePath\":null,\"ArchiveStorageFileName\":\"FundTradeType\",\"BronzeWorkspaceName\":\"Dev - Crimson\",\"BronzeLakehouseName\":\"lh_bronze\",\"BronzeSchemaName\":\"Bronze\",\"BronzeTableName\":\"CrimsonXFundTradeType\",\"BronzeLoadMethod\":\"overwrite\",\"BronzeWorkspaceId\":null,\"BronzeLakehouseId\":null,\"WatermarkColumn\":\"ETLLoadDateTime\",\"SinkTableName\":\"CrimsonXFundTradeType\",\"SinkSchemaName\":\"Bronze\",\"SinkWatermarkColumn\":\"ETLLoadDateTime\",\"SinkLoadMethod\":\"overwrite\",\"IsWatermarkEnabledFlag\":false}"
GlobalConfig = "[{\"ConfigKey\":\"SPJobAuditStartLock\",\"ConfigValue\":\"0\"},{\"ConfigKey\":\"CuratedLakehouseId\",\"ConfigValue\":\"e9fc4e80-ff69-4d45-bbdd-892592889465\"},{\"ConfigKey\":\"BronzeLakehouseId\",\"ConfigValue\":\"13ef97da-5da2-466d-8c5f-2a70572c6558\"},{\"ConfigKey\":\"Dev - Crimson\",\"ConfigValue\":\"33535eb8-4d07-49bc-b3a5-cc91d3aa6ced\"},{\"ConfigKey\":\"RawLakehouseId\",\"ConfigValue\":\"920a12cc-7104-4013-a2a3-d7baa57e9e3f\"}]"



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

#TaskList = ''
#WatermarkList = ''
#GlobalConfig = ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(TaskList)
# Convert JSON String to list
tasklist = [json.loads(TaskList)]
# watermarklist = json.loads(WatermarkList)
guidlist = json.loads(GlobalConfig)

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

def process_task(task, etlloadtime):
    try:
        print(f"Processing TaskKey: {task['TaskKey']}. Sink: {task['SinkTableName']}")

        # Define Parameters
        source_path = f"{raw_lh_basepath}/Files/{task['RawStoragePath']}/{task['SourceTableName']}"
        print(f"Reading from: {source_path}")
        target_table = task['SinkTableName']
        target_schema = task['SinkSchemaName']
        target_path = f"{bronze_lh_basepath}/Tables/{target_schema}/{target_table}"
        hwm_column = task['SinkWatermarkColumn']
        primary_column = task['PrimaryKeyColumnList']
        method = task['SinkLoadMethod']
        # hwm_value = next((item["HighWatermarkValue"] for item in watermarklist if item["TaskKey"] == task["TaskKey"]), None)


        # Try reading the original path
        try:
            df_source = spark.read.format("parquet").load(source_path +"/")
        except AnalysisException:
            # Try appending ".parquet" if the first attempt fails
            try:
                df_source = spark.read.format("parquet").load(source_path + ".parquet")
            except AnalysisException as e:
                raise FileNotFoundError(f"Parquet file not found at '{source_path}' or '{source_path}.parquet'.") from e

        RowsRead = df_source.count()

        # Determine primary keys: if primary_column is None, use all columns as primary keys
        if not primary_column:
            primary_keys = df_source.columns  # all columns
        else:
            primary_keys = [col.strip() for col in (primary_column.split(",") if isinstance(primary_column, str) else primary_column)]

        # Deduplicate based on the primary keys
        df_source = df_source.dropDuplicates(primary_keys)
        print(f"[debug] Columns after reading and dedup: {df_source.columns}")

        df_source, rename_map = sanitize_column_names_with_mapping(df_source)
        print(f"[debug] Columns after sanitizing: {df_source.columns}")

        df_source = unify_hwm_column(df_source, hwm_column)
        print(f"[debug] Columns after unifying: {df_source.columns}")

        df_source.createOrReplaceTempView("df_source_view")


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

            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {target_schema}.{target_table}
                USING DELTA
                LOCATION '{target_path}'
            """)

        elif method == 'overwrite':
            print(f"\nOverwriting existing table at: {target_path}")
            if hwm_column not in df_source.columns:
                try:
                    df_source = df_source.withColumn(hwm_column, lit(etlloadtime))
                except Exception as e:
                    print(f"[ERROR] Failed to add watermark column '{hwm_column}': {e}")
                    raise
            df_source.write.format("delta").mode("overwrite").save(target_path)

        elif method == 'append':
            print(f"\nAppending to existing table at: {target_path}")
            if hwm_column not in df_source.columns:
                try:
                    df_source = df_source.withColumn(hwm_column, lit(etlloadtime))
                except Exception as e:
                    print(f"[ERROR] Failed to add watermark column '{hwm_column}': {e}")
                    raise
            df_source.write.format("delta").mode("append").save(target_path)
        
        else:
            print(f"Method is not overwrite or append, '{method}' — performing merge")
        # Get schema information
        df_source_view_df = spark.table("df_source_view")
        schema_target = {field.name: field.dataType for field in df_target.schema if field.name != hwm_column}
        schema_source = {field.name: field.dataType for field in df_source_view_df.schema if field.name != hwm_column}

        # Schema doesn't match
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
        columns = df_source.columns
        change_condition = " OR ".join([f"target.{col} IS DISTINCT FROM source.{col}" for col in columns if col not in primary_keys and col != hwm_column])

        # Check if all columns are primary keys by comparing with the primary_column list
        if len(primary_keys) == len(columns):
            # All columns are primary keys, meaning only inserts should happen unless data changes.
            print(f"All columns are primary keys. Ensuring valid update conditions.")

            merge_query = f"""
            MERGE INTO {target_schema}.{target_table} AS target
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

            merge_query += f"""
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(columns)}, {hwm_column})
                VALUES ({', '.join([f'source.{col}' for col in columns])}, '{etlloadtime}'); 
            """

            print(merge_query)


        else:
            # Regular merge logic (non-primary-key columns can be updated)
            merge_query = f"""
            MERGE INTO {target_schema}.{target_table} AS target
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
        # Get workspace and lakehouse keys from task item
        bronze_ws_key = task_item.get('BronzeWorkspaceName')
        raw_ws_key = task_item.get('BronzeWorkspaceName')  # Assuming same key used, correct if different

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
        result = process_task(task_item, etlloadtime)
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

from datetime import datetime
import re
from notebookutils import mssparkutils  # Import Fabric-specific file system utilities

# Archive path (already created earlier in the code)
archive_path = f"{raw_lh_basepath}/Files/Archive"
print(archive_path)

def move_to_archive(file_path):
    file_path = file_path.rstrip('/')  # Remove trailing slash
    print(f"Processed file path: {file_path}")
    
    # Get current date (year, month, day)
    current_date = datetime.now()
    year = current_date.year
    month = current_date.strftime('%m')  # Zero-padded month
    day = current_date.strftime('%d')   # Zero-padded day

    # Extract folder name like "01Test" from the input file path
    match = re.search(r'/Files/([^/]+)(?:/|$)', file_path)

    print(f"Regex match result: {match}")
    
    if not match:
        print(" Unable to extract source folder (e.g., 01Test) from path.")
        return
    
    table_folder = match.group(1)
    print(f"Extracted folder: {table_folder}")

    # Construct full archive destination path
    archive_dir = f"{archive_path}/{year}/{month}/{day}"
    file_name = file_path.split('/')[-1]  # Extract the file name from the path
    destination_path = f"{archive_dir}"

    print(f"Moving to: {destination_path}")

    try:
        # Ensure the destination directory exists in Fabric
        # In Fabric, use mssparkutils.fs.mkdirs for directory creation (works with ADLS/OneLake)
        mssparkutils.fs.mkdirs(archive_dir)

        # Use mssparkutils.fs.mv() to move the file in Fabric
        mssparkutils.fs.mv(file_path, destination_path)

        # Delete the original after confirming copy
        mssparkutils.fs.rm(file_path, recurse=False)


        print(f"File moved successfully to: {destination_path}")
    except Exception as e:
        print(f"Error occurred while moving file: {e}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# result = {
#     "RowsCopied": rows_copied,
#     "RowsInserted": rows_inserted,
#     "RowsUpdated": rows_updated,
#     "RowsDeleted": rows_deleted
# }
# notebookutils.notebook.exit(str(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
