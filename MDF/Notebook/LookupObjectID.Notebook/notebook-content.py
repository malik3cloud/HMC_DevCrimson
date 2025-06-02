# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

WorkspaceName = "Crimson - Dev"
NotebookName = None
LakehouseName = None

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

# functions below are from utils notebook
workspace_id = get_workspace_id(
    workspace_name=WorkspaceName
)

notebook_id = get_notebook_id(
    workspace_name=WorkspaceName,
    notebook_name=NotebookName
)

lakehouse_id = get_lakehouse_id(
    workspace_name=WorkspaceName, 
    lakehouse_name=LakehouseName
)
LOGGER.info(f"workspace_id={workspace_id}; notebook_id={notebook_id}; lakehouse_id={lakehouse_id}")


result = {
    "WorkspaceId": workspace_id,
    "NotebookId": notebook_id,
    "LakehouseId": lakehouse_id
}
notebookutils.notebook.exit(str(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
