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
# META         }
# META       ]
# META     }
# META   }
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

# -----------------------------------
# FUNCTION TO RUN NOTEBOOK BY PATH
# -----------------------------------

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

 # mssparkutils.notebook.run("NotebookSilverTest2", 600)
 
#  mssparkutils.notebook.run("NotebookSilverTest1", 600,  {'useRootDefaultLakehouse': True})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------
# FUNCTION TO RUN NOTEBOOK BY PATH
# -----------------------------------
def run_notebook_by_path(notebook_path: str, timeout: int = 600, params: dict = {}) -> str:
    try:
        result = mssparkutils.notebook.run(notebook_path, timeout, params)
        return f"✅ SUCCESS: {notebook_path} → {result}"
    except Exception as e:
        return f"❌ FAILED: {notebook_path} → {e}"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------
# SEQUENTIAL EXECUTION
# -----------------------------------
print("🔁 Starting SEQUENTIAL notebook execution...")
sequential_notebooks = [
     "NotebookSilverTest1",
      "NotebookSilverTest2"
 
]

for path in sequential_notebooks:
    result = run_notebook_by_path(path)
    print(result)
    if "FAILED" in result:
        print("❌ Stopping further execution due to failure.")
        break


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -----------------------------------
# PARALLEL EXECUTION
# -----------------------------------
from concurrent.futures import ThreadPoolExecutor, as_completed

print("\n⚡ Starting PARALLEL notebook execution...")
parallel_notebooks = [

]


with ThreadPoolExecutor(max_workers=len(parallel_notebooks)) as executor:
    futures = {executor.submit(run_notebook_by_path, path): path for path in parallel_notebooks}
    for future in as_completed(futures):
        print(future.result())


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
