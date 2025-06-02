# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Get workspace id from workspace name
import sempy.fabric as fabric



def get_workspace_id(workspace_name):
    # try:
    #     workspaces = fabric.list_workspaces()
    #     return workspaces[workspaces['Name'] == workspace_name]['Id'][0]
    # except:
    #     # raise Exception("Workspace does not exist")
    #     return str()

    try:
        workspaces = fabric.list_workspaces()
        
        match = workspaces[workspaces['Name'] == workspace_name]
        if match.empty:
            raise ValueError(f"Workspace '{workspace_name}' not found.")
        
        return match['Id'].iloc[0]
    
    except Exception as e:
        print(f"[ERROR] Failed to get workspace ID: {e}")
        raise

def get_notebook_id(workspace_name, notebook_name):
    try:
        workspace_id = get_workspace_id(workspace_name)
        items = fabric.list_items(workspace=workspace_id)
        notebook_id = items[(items["Type"] == "Notebook") & (items["Display Name"] == notebook_name)]["Id"].iloc[0]
        return notebook_id
    except Exception as e:
        # raise Exception("Notebook does not exist")
        return str()

def get_lakehouse_id(workspace_name, lakehouse_name):
    try:
        workspace_id = get_workspace_id(workspace_name)
        items = fabric.list_items(workspace=workspace_id)
        notebook_id = items[(items["Type"] == "Lakehouse") & (items["Display Name"] == lakehouse_name)]["Id"].iloc[0]
        return notebook_id
    except Exception as e:
        # raise Exception("Lakehouse does not exist")
        return str()

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

# CELL ********************

spark.conf.set("spark.ms.autotune.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Logger
import logging

FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
formatter = logging.Formatter(fmt=FORMAT)
for handler in logging.getLogger().handlers:
    handler.setFormatter(formatter)

logging.getLogger().setLevel(logging.INFO)
LOGGER = logging.getLogger(notebookutils.runtime.context["currentNotebookName"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
