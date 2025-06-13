-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse_name": "",
-- META       "default_lakehouse_workspace_id": ""
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "cd7a9253-3c23-bb87-4183-182195db441c",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "cd7a9253-3c23-bb87-4183-182195db441c",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- ## Switch to T-SQL

-- MARKDOWN ********************

-- The code for T-SQL below is to query the max task key of the ETL table you are trying to populate there are a couple of alternatives to get this values such as 1. querying it from Warehouse directly or 2.  manually inspecting the table for max task key (not recommended if your table has many rows, warehouse UI might not display all)

-- CELL ********************

-- Execute a Data Warehouse query using T-SQL; 
-- You can run this here via notebook by switching to T-SQL or directly query ETL Warehouse for max taskkey
DECLARE @schema_name NVARCHAR(128) = 'etl';
DECLARE @table_name NVARCHAR(128) = 'Task'; -- or FileTask or Job, change according to what table you're trying to populate
DECLARE @sql_max NVARCHAR(MAX);

SET @sql_max = N'SELECT MAX(TaskKey) AS max_task_key FROM ' + QUOTENAME(@schema_name) + N'.' + QUOTENAME(@table_name);

EXEC sp_executesql @sql_max;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- ## Switch to PySpark

-- MARKDOWN ********************

-- The codes below will automatically generale sql queries that you can excute manually against the etl warehouse. This notebook is to trim down the work when inserting in bulk.

-- CELL ********************

from pyspark.sql import SparkSession
import pandas as pd
import json
from notebookutils import mssparkutils

# Initialize Spark (if not already initialized)
spark = SparkSession.builder.appName("ExcelToJson").getOrCreate()

# Step 1: Load Excel file
excel_path = "./builtin/ETLTable.xlsx"  # Change this to a path / builtin is not accessible via pipeline

# Read the Excel file into Pandas (for better compatibility with Excel)
pdf = pd.read_excel(excel_path, sheet_name=0)  # Read the first sheet

# Convert Pandas DataFrame to Spark DataFrame
df = spark.createDataFrame(pdf)

# Step 2: Convert Spark DataFrame to JSON String
json_str = df.toJSON().collect()  # Convert each row to JSON and collect as list
json_output = json.dumps(json_str, indent=2) 

# Step 3: Display Output
print(json_output)


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

# Return JSON object
mssparkutils.notebook.exit(json.dumps(json_output))

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

# Set the variable manually from the result above (max_task_key)
max_task_key = 11 # set to 0 if first time running

# Set this for task or other etl table
key = "JobKey" # change to the key column of the table you're trying to populate
table_name = "Job" # or FileTask or Job, change according to what table you're trying to populate
schema_name = "etl"

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

import pandas as pd

def generate_insert_statements(excel_file, table_name, schema_name, key):
    # Load the Excel file, the taskkeys here will be ignored
    df = pd.read_excel(excel_file)
    
    # Get column names from the first row
    columns = df.columns.tolist()

    # Ensure TaskKey column exists
    if key not in columns:
        raise ValueError("Key specified not found.")

    
     # Generate SQL INSERT statements
    sql_statements = []
    key_main = max_task_key + 1  # Start from max_task_key + 1

    for _, row in df.iterrows():
        values = []
        for col, value in zip(columns, row):
            if col == key:  
                values.append(str(key_main))  # Assign incremental TaskKey
                key_main += 1  # Increment for the next row
            elif pd.isna(value):
                values.append("NULL")
            elif isinstance(value, str):
                 values.append(f"'{value}'")
            else:
                values.append(str(value))
        
        values_str = ", ".join(values)
        # Use this if you will save this in a sql file
        sql = f"INSERT INTO [{schema_name}].[{table_name}] ({', '.join(columns)}) VALUES ({values_str});"
        sql_statements.append(sql)
    
    return sql_statements


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
