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

# PARAMETERS CELL ********************

WorkspaceName = "Dev - Crimson"
NotebookName = 'KnowBe4PhishingData'
LakehouseName = 'lh_raw'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import necessary libraries
from concurrent.futures import ThreadPoolExecutor, as_completed
from notebookutils import mssparkutils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def run_notebook_by_path(notebook_path: str, timeout: int = 600, params: dict = {}) -> str:
    try:
        result = mssparkutils.notebook.run(notebook_path, timeout, params)
        return f"SUCCESS: {notebook_path} → {result}"
    except Exception as e:
        return f"FAILED: {notebook_path} → {e}"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = run_notebook_by_path(NotebookName)
print(result)
if "FAILED" in result:
   print("❌ Stopping further execution due to failure.")
  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
