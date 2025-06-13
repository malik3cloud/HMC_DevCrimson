# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0d258a24-e449-43e6-a029-4a56ec727d07",
# META       "default_lakehouse_name": "LakehouseSilver",
# META       "default_lakehouse_workspace_id": "8d1661d3-1509-409e-96e6-2f2d2da983de",
# META       "known_lakehouses": [
# META         {
# META           "id": "0d258a24-e449-43e6-a029-4a56ec727d07"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

from datetime import datetime, date
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, row_number, current_timestamp, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from pyspark.sql.window import Window

# TODO: keep or delete this: may want to be parameter driven to work on a single or a given list of index IDs
index_id_list = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# this cell creates dummy data (some were taken from initial version of the gap fill notebook)
# to productionalize this, we will need more tables/data in Fabric

# Initialize Spark session if needed
spark = SparkSession.builder.getOrCreate()

# simulate a new data load (after gaps filled). For Bloomberg sourced data, whether index is level or return, for the raw table, all numeric values are in the "IndexLevel" column
# however, manually sourced raw data has two columns: index level and index return. Account for this. also account for if a return is like 5.0 or 0.05.
schema = StructType([
    StructField("IndexId", StringType(), True),
    StructField("FrequencyId", IntegerType(), True),
    StructField("AsOfDate", DateType(), True),
    StructField("CurrencyId", IntegerType(), True),
    StructField("IndexLevel", DoubleType(), True),
    StructField("IndexReturn", DoubleType(), True),
])
data = [
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 16), 1, 1075.1111, None), # note that 8 = Daily
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 17), 1, 1075.1111, None),
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 18), 1, 1075.1111, None),
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 19), 1, 1088.2222, None),
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 20), 1, 1088.2222, None),
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 21), 1, 1088.2222, None),
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 22), 1, 1075.1111, None),
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 23), 1, 1075.1111, None),
    ("another index ID",                   8, date(2024, 12, 10), 1, 123.456,   None),
    ("another index ID",                   8, date(2024, 12, 11), 1, 123.456,   None),
    ("another index ID",                   8, date(2024, 12, 12), 1, 123.456,   None),
    ("another index ID",                   8, date(2024, 12, 13), 1, 123.456,   None),
    ("another index ID",                   8, date(2024, 12, 14), 1, 123.456,   None),
    ("another index ID",                   8, date(2024, 12, 15), 1, 133.444,   None),
    ("return based monthly index ID",      2, date(2025,  1, 31), 1, 0.0131, None),  # note 1.31 or 0.0131? (BB will be 0.0131, user raw file will be 1.31)
    ("return based monthly index ID",      2, date(2025,  2, 28), 1, 0.0228, None),  # note that 2 = Monthly
    ("return based monthly index ID",      2, date(2025,  3, 31), 1, 0.0331, None)
]
df_raw = spark.createDataFrame(data, schema=schema)
df_raw.show(truncate=False)

# simulate "Index Load Control" table in staging that contains index metadata
schema = StructType([
    StructField("IndexId", StringType(), True), 
    StructField("CurrencyIdBase", IntegerType(), True),
    StructField("FrequencyIdBase", IntegerType(), True),
    StructField("VendorIndexReturnUnit", StringType(), True)
])
data = [
    ("21d419ba-9a21-ea11-8133-0050569a56ba",1,8,'LEVEL'),
    ("another index ID",                    1,8,'LEVEL'),
    ("return based monthly index ID",       1,2,'RETURN')
]
df_ilc = spark.createDataFrame(data, schema=schema)
df_ilc.show(truncate=False)

# simulate the "unvierse" of index IDs we are working with
schema = StructType([StructField("IndexId", StringType(), True)])
data = [
    ("21d419ba-9a21-ea11-8133-0050569a56ba",),
    ("another index ID",),
    ("return based monthly index ID",)
]
df_universe = spark.createDataFrame(data, schema=schema)
df_universe.show(truncate=False)

# simulate historical index returns (note warehouse has 12MM rows)
# this simulated data is for one day earlier than the test data we are loading
schema = StructType([
    StructField("IndexId", StringType(), True),
    StructField("CurrencyId", IntegerType(), True),
    StructField("FrequencyId", IntegerType(), True),
    StructField("AsOfDate", DateType(), True),
    StructField("IndexLevel", DoubleType(), True),
    StructField("IndexReturn", DoubleType(), True)
])
data = [
    ("21d419ba-9a21-ea11-8133-0050569a56ba",1,8,datetime.strptime("05/15/2025", "%m/%d/%Y").date(), 1065.000,  0.0),
    ("another index ID",                    1,8,datetime.strptime("12/09/2024", "%m/%d/%Y").date(),  111.000,  0.0),
    ("return based monthly index ID",       1,2,datetime.strptime("12/31/2024", "%m/%d/%Y").date(),  100.000, 0.0)
   ]
df_wh = spark.createDataFrame(data, schema=schema)
df_wh.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# this cell overwrites the test data from above with slightly better test data
# including reading full 12MM rows of IndexReturn data from delta table

# simulate a new data load (after gaps filled). For Bloomberg sourced data, whether index is level or return, for the raw table, all numeric values are in the "IndexLevel" column
# however, manually sourced raw data has two columns: index level and index return. Account for this. also account for if a return is like 5.0 or 0.05.
schema = StructType([
    StructField("IndexId", StringType(), True),
    StructField("FrequencyId", IntegerType(), True),
    StructField("AsOfDate", DateType(), True),
    StructField("CurrencyId", IntegerType(), True),
    StructField("IndexLevel", DoubleType(), True),
    StructField("IndexReturn", DoubleType(), True),
])
data = [
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 16), 1, 1075.1111, None), # note that 8 = Daily
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 17), 1, 1075.1111, None),
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 18), 1, 1075.1111, None),
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 19), 1, 1088.2222, None),
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 20), 1, 1088.2222, None),
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 21), 1, 1088.2222, None),
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 22), 1, 1075.1111, None),
    ("21d419ba-9a21-ea11-8133-0050569a56ba", 8, date(2025, 5, 23), 1, 1075.1111, None),
    ("another index ID",                   8, date(2024, 12, 10), 1, 123.456,   None),
    ("another index ID",                   8, date(2024, 12, 11), 1, 123.456,   None),
    ("another index ID",                   8, date(2024, 12, 12), 1, 123.456,   None),
    ("another index ID",                   8, date(2024, 12, 13), 1, 123.456,   None),
    ("another index ID",                   8, date(2024, 12, 14), 1, 123.456,   None),
    ("another index ID",                   8, date(2024, 12, 15), 1, 133.444,   None),
    ("return based monthly index ID",      2, date(2025,  1, 31), 1, 0.0131, None),  # note 1.31 or 0.0131? (BB will be 0.0131, user raw file will be 1.31)
    ("return based monthly index ID",      2, date(2025,  2, 28), 1, 0.0228, None),  # note that 2 = Monthly
    ("return based monthly index ID",      2, date(2025,  3, 31), 1, 0.0331, None)
]
df_raw = spark.createDataFrame(data, schema=schema)
df_raw.show(truncate=False)

# simulate "Index Load Control" table in staging that contains index metadata
schema = StructType([
    StructField("IndexId", StringType(), True), 
    StructField("CurrencyIdBase", IntegerType(), True),
    StructField("FrequencyIdBase", IntegerType(), True),
    StructField("VendorIndexReturnUnit", StringType(), True)
])
data = [
    ("21d419ba-9a21-ea11-8133-0050569a56ba",1,8,'LEVEL'),
    ("another index ID",                    1,8,'LEVEL'),
    ("return based monthly index ID",       1,2,'RETURN')
]
df_ilc = spark.createDataFrame(data, schema=schema)
df_ilc.show(truncate=False)

# simulate the "unvierse" of index IDs we are working with
schema = StructType([StructField("IndexId", StringType(), True)])
data = [
    ("21d419ba-9a21-ea11-8133-0050569a56ba",),
    ("another index ID",),
    ("return based monthly index ID",)
]
df_universe = spark.createDataFrame(data, schema=schema)
df_universe.show(truncate=False)

# simulate historical index returns (note warehouse has 12MM rows)
# this simulated data is for one day earlier than the test data we are loading
schema = StructType([
    StructField("IndexId", StringType(), True),
    StructField("CurrencyId", IntegerType(), True),
    StructField("FrequencyId", IntegerType(), True),
    StructField("AsOfDate", DateType(), True),
    StructField("IndexLevel", DoubleType(), True),
    StructField("IndexReturn", DoubleType(), True)
])
data = [
    ("21d419ba-9a21-ea11-8133-0050569a56ba",1,8,datetime.strptime("05/15/2025", "%m/%d/%Y").date(), 1065.000,  0.0),
    ("another index ID",                    1,8,datetime.strptime("12/09/2024", "%m/%d/%Y").date(),  111.000,  0.0),
    ("return based monthly index ID",       1,2,datetime.strptime("12/31/2024", "%m/%d/%Y").date(),  100.000, 0.0)
   ]
df_wh = spark.createDataFrame(data, schema=schema)
df_wh.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#TODO: experiment with reading the full 12MM row delta table in to a dataframe
# this took 18 seconds... no issues so far

#overwrite the dataframe from above with real data
df_wh = spark.read.format("delta").load("Tables/IndexReturnWH")

df_wh.show(10)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# build a dataframe for staging the data from the raw data and flag data appropriately

# need to know new data being loaded.  designate as Raw
# note that these Raw rows may replace rows in our index returns table
df_stage = df_raw.withColumn("Source", lit("Raw"))
df_stage = df_stage.withColumn("IsValid", lit("Y")) # use IsValid to flag if a row is good or not

# need to know which rows were brought in as the "prior date" data point needed for computation. designate as Historical
# note that these Historical rows will not get to our index returns table (they are already there)
df_wh = df_wh.withColumn("Source", lit("Historical"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# join with the index load control metadata so we know if an index is level based or return based
df_stage = df_stage.alias("stage")
df_ilc = df_ilc.alias("ilc")

df_stage = df_stage.join(
    df_ilc,
    col("stage.IndexId") == col("ilc.IndexId"),
    how="left"
).select("stage.*", "ilc.VendorIndexReturnUnit")

df_stage.show(100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# The HMC index SLA states that we need to have both a level and a return for each date
# Consider a level based index. In order to compute a "return" for a new level data point, we need the value for the prior level
# When we have a new data load, we need to query the historic data to get the prior data point 

# start by identifing the earliest date for each grouping in our new/raw data
min_dates = df_stage\
    .join(df_universe, "IndexId") \
    .groupBy("IndexId", "CurrencyId", "FrequencyId") \
    .agg(spark_min("AsOfDate").alias("AsOfDate"))

min_dates.show(10)

#TODO:... maybe write spark sql
# now query the historic data for the max date that is prior to the above min dates
start_rows = df_wh.alias("ir").join(
    min_dates.alias("mindate"),
    (col("ir.IndexId") == col("mindate.IndexId")) &
    (col("ir.CurrencyId") == col("mindate.CurrencyId")) &
    (col("ir.FrequencyId") == col("mindate.FrequencyId")) &
    (col("ir.AsOfDate") < col("mindate.AsOfDate"))
).groupBy("ir.IndexId", "ir.CurrencyId", "ir.FrequencyId") \
 .agg(spark_max("ir.AsOfDate").alias("AsOfDate"))

start_rows.show(100)

# Join in order to get the historic data with desired columns
wh = df_wh.alias("wh")
ilc = df_ilc.alias("ilc")
sr = start_rows.alias("sr")
start_data = wh.join(
    sr,
    on=["IndexId", "CurrencyId", "FrequencyId", "AsOfDate"],
    how="inner"
).join(
    ilc,
    col("wh.IndexId") == col("ilc.IndexId"),
    how="inner"
).select(
    col("wh.IndexId"),
    col("wh.CurrencyId"),
    col("wh.FrequencyId"),
    col("wh.AsOfDate"),
    col("wh.IndexLevel"),
    col("wh.IndexReturn"),
    col("ilc.VendorIndexReturnUnit"),
    col("wh.Source")
)
# flag this data as not valid because we do not want to move it to move to next medallion
start_data = start_data.withColumn("IsValid", lit("N"))
start_data.show(100)

# Union the raw/stage data with the historic data
df_stage = df_stage.unionByName(start_data)

# separate out return based data into a separate dataframe
df_stage_return_based = df_stage.filter(col("VendorIndexReturnUnit") == "RETURN")

# keep level based in df_stage
df_stage = df_stage.filter(col("VendorIndexReturnUnit") == "LEVEL")

df_stage.show(100)


#TODO: temp view... like a logical view of a data frame
#TODO: temp view... option to keep in memory... may have memory issues.  maybe use delta table

#TODO: pandas... in memory

#TODO: experiment by bringing the 12MM row into a dataframe


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Add Row Numbers and Calculate LEVEL-Based Returns

# Assign Row Numbers per grouping
# Equivalent to: ROW_NUMBER() OVER (PARTITION BY IndexId, CurrencyId, FrequencyId ORDER BY AsOfDate)
window_spec = Window.partitionBy("IndexId", "CurrencyId", "FrequencyId").orderBy("AsOfDate")
df_stage = df_stage.withColumn("RowNum", row_number().over(window_spec))

# Join with previous row to compute LEVEL-based returns
# curr.IndexReturn = (curr.IndexLevel - prev.IndexLevel) / prev.IndexLevel
df_prev = df_stage.withColumnRenamed("RowNum", "RowNumPrev") \
                  .withColumnRenamed("IndexLevel", "IndexLevelPrev")

df_joined = df_stage.alias("curr").join(
    df_prev.alias("prev"),
    (col("curr.IndexId") == col("prev.IndexId")) &
    (col("curr.CurrencyId") == col("prev.CurrencyId")) &
    (col("curr.FrequencyId") == col("prev.FrequencyId")) &
    (col("curr.RowNum") == col("prev.RowNumPrev") + 1)
).withColumn(
    "CalcIndexReturn",
    when(col("prev.IndexLevelPrev") == 0, lit(0.0))
    .otherwise((col("curr.IndexLevel") - col("prev.IndexLevelPrev")) / col("prev.IndexLevelPrev"))
)

df_joined.show(100)

# get back to desired set of columns
df_stage = df_joined.select("curr.IndexId", "curr.FrequencyId", "curr.AsOfDate", "curr.CurrencyId", "curr.IndexLevel",
  col("CalcIndexReturn").alias("IndexReturn"), 
  "curr.IsValid", "curr.VendorIndexReturnUnit")

df_stage.show(100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# for RETURN based index returns, calculate levels

#for raw BB souced data, all numeric values are in column IndexLevel
#but, for a return based index, we need to put that data in the IndexReturn column and then compute the level
#TODO: this code will need to change for a manually sources .xlsx index return file

# for Historical data point, need to presere the level
df_stage_return_based  = df_stage_return_based.withColumn(
    "IndexLevelNEW",
    when(
        (col("Source") == "Historical"), col("IndexLevel")
    ).otherwise(None))

df_stage_return_based = df_stage_return_based.withColumn(
    "IndexReturn",
    when(
        (col("Source") == "Historical"), None
    ).otherwise(col("IndexLevel")))

# Assign RowNum per Index group
window_spec = Window.partitionBy("IndexId", "CurrencyId", "FrequencyId").orderBy("AsOfDate")
df_stage_return_based = df_stage_return_based.withColumn("RowNum", row_number().over(window_spec))

#TODO: Pandas is python library, used for calculations and analysis and data manipulation
#TODO: consider "window function"/recursion... may be faster
#TODO: SQL? pySQL. move dataframe to a temporary view... like temp table in sql
# Convert to Pandas for group-based recursion
pdf = df_stage_return_based.toPandas()

# Apply recursion per group
for keys, group in pdf.groupby(["IndexId", "CurrencyId", "FrequencyId"]):
    group = group.sort_values("RowNum")
    levels = []
    for i, row in group.iterrows():
        #print(row)
        if row["RowNum"] == 1:
            # Default starting value if not provided
            # TODO: how to handle this case?
            #level = 100000.0 if pd.isna(row["IndexLevel"]) else row["IndexLevel"]
            level = row["IndexLevelNEW"]
        else:
            #print(levels[-1])
            #level = levels[-1] * (1 + row["IndexReturn"]/100.0) #TODO: handle if we need to divide by 100.0 or now
            level = levels[-1] * (1 + row["IndexReturn"] )
        levels.append(level)
    #print(levels)
    pdf.loc[group.index, "IndexLevelNEW"] = levels
#print(levels)

# Convert back to Spark DataFrame
df_stage_return_based = spark.createDataFrame(pdf)

# keep just Raw data (gets rid of "Historical")
df_stage_return_based = df_stage_return_based.filter(col("Source") == "Raw")

df_stage_return_based.show(100)

# clean up columns
df_stage_return_based = df_stage_return_based.select("IndexId", "FrequencyId", "AsOfDate", "CurrencyId", 
  col("IndexLevelNEW").alias("IndexLevel"), "IndexReturn", "IsValid", "VendorIndexReturnUnit")

df_stage_return_based.show(100)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

# combine the return based data with the level based data (already in df_stage)
df_stage = df_stage.union(df_stage_return_based)

# this is final product that needs to merge with next medallion 
df_stage.show(100)


# Apply data quality rules (IsValid = 'N' where essential values are missing)
# Equivalent to:
#   - If IndexReturn IS NULL AND ReturnUnit = 'LEVEL' → Invalid
#   - If IndexLevel IS NULL AND ReturnUnit = 'RETURN' → Invalid

# df_joined = df_joined.withColumn(
#     "IsValid2",
#     when(
#         (col("curr.IndexReturn").isNull()) & (col("curr.VendorIndexReturnUnit") == "LEVEL"),
#         "N"
#     ).otherwise(col("curr.IsValid"))
# )

# df_joined = df_joined.withColumn(
#     "IsValid3",
#     when(
#         (col("curr.IndexLevel").isNull()) & (col("curr.VendorIndexReturnUnit") == "RETURN"),
#         "N"
#     ).otherwise(col("prev.IsValid"))
# )

# df_init_stage.show(100)
# df_joined.select("curr.*").show(100)
# df_joined.show(100)

#df_joined.printSchema()

# Save result to Delta table (OVERWRITE mode)
#df_stage.write.format("delta").mode("overwrite").save("Tables/IndexReturn_STAGE")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
