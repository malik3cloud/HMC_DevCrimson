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

import sempy.fabric as fabric

workspace_name = "Dev - Crimson"
item_name = "[Test]NBLoadRawToBronze"

# Get workspace ID from name
workspace_id = fabric.resolve_workspace_id(workspace_name)

# List all items in the workspace
items = fabric.list_items(workspace=workspace_id)

# Filter to only notebooks
notebooks = items[items["Type"] == "Notebook"].dropna()

# Display the notebook list
display(notebooks)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
