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
from pyspark.sql.functions import lit, col
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
NotebookList = ''
CuratedLhId = ''
BronzeLhId = ''
WorkspaceId = ''
RawLhId= ''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert JSON String to list
tasklist = [json.loads(TaskList)]
# watermarklist = json.loads(WatermarkList)
nblist = json.loads(NotebookList)

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

def get_name_by_key(nblist: list[dict], nbkey: int) -> str:
    df = spark.createDataFrame(json_list)
    result = df.filter(col("NotebookKey") == nbkey).select("NotebookName").collect()
    return "NotFound" if result else None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for task_item in tasklist:
    try:
        # Build paths
        bronze_lh_basepath = get_basepath(WorkspaceId, BronzeLhId)
        raw_lh_basepath = get_basepath(WorkspaceId, RawLhId)
        curated_lh_basepath = get_basepath(WorkspaceId, CuratedLhId)

        # Process the task
        notebook_name = get_name_by_key(nblist, task_item['NotebookKey'])
        result = mssparkutils.notebook.run(notebook_name, arguments={
            "bronze_lh_basepath": bronze_lh_basepath,
            "raw_lh_basepath": raw_lh_basepath,
            "curated_lh_basepath" : curated_lh_basepath,
            "TargetTable": task_item['SinkTableName']
        })

    except Exception as e:
        
        print(f"Error processing task: {e}")
        raise Exception

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
