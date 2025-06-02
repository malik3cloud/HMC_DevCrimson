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

df = spark.read.parquet("Files/INSERT Scripts/etlTask")  # Replace with actual path
df.show()
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def generate_insert_script(df, table_name):
    insert_statements = []
    columns = df.columns
    for row in df.collect():
        values = []
        for value in row:
            if value is None:
                values.append("NULL")
            elif isinstance(value, str):
                escaped = value.replace("'", "''")  # Escape single quotes for SQL
                values.append(f"'{escaped}'")
            else:
                values.append(str(value))
        col_str = ", ".join(columns)
        val_str = ", ".join(values)
     #   table_name = "etl.Task" 
        insert_statements.append(f"INSERT INTO {table_name} ({col_str}) VALUES ({val_str});")
    return insert_statements

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.parquet("Files/INSERT Scripts/etlTask")  # Or a table

sql_statements = generate_insert_script(df, "etl.Task")

# Preview some
for stmt in sql_statements[:5]:
    print(stmt)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#dbutils.fs.put("Files/INSERT Scripts/etlTask.sql", "\n".join(sql_statements), overwrite=True)

from notebookutils import mssparkutils  # Only in Microsoft Fabric

# Write SQL to a file in the Lakehouse Files section
output_path = "Files/INSERT Scripts/etlTask.sql"
sql_content = "\n".join(sql_statements)

mssparkutils.fs.put(output_path, sql_content, overwrite=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
