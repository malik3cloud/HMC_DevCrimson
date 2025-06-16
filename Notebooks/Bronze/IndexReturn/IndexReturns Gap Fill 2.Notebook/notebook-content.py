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

#purpose of this notebook is to start with index return data that is missing some dates (e.g. weekends)
#and produce a continous set of data points
#setup dummy data for testing. include a few dates with gaps
#note that real data would have a mixture of index IDs, frequencies and currencies (and dates)
#also, some indices report as level and some as return
#so, we need to handle rolling forward of both level and/or return

#start by just rolling forward level and only handle minimal permutations... only one currency, one frequency

#todo: handle multiple currencies
#todo: handle multiple frequencies
#todo: handle rollowing forward returns if index reports as return
#todo: handle different data sources/reasons: Bloomberg nightly load, Bloomberg new index since inception, SOIL load, use generated manual benchmark files
#todo: add data quality checks: BB data can have error codes
#todo: what else?

# Version History
# 1 basic functionality demonstrates that logic works for canned data
# 2 migrated to Dev - Crimson and change to read parquet file

from pyspark.sql import SparkSession
from datetime import date
from pyspark.sql.functions import col, lit, to_date, trim

base_path = "Files/IndexReturn"
filename = "BloombergIndexReturn"
full_path = f"{base_path}/{filename}"

# xxx todo: need to process "Status" column here and flag any non-zero values
# xxx todo: also need to flag any blank Date or Level values which can occur
# xxx todo: in both cases, filter out bad rows and report issue

df_stage = spark.read.parquet(full_path)

#xxx flag/report/purvue these issues
df_issues = df_stage.filter(
    (col("Status") != 0) |
    (col("Date").isNull()) &
    (trim(col("Date")) == "") &
    (col("Level").isNull()) &
    (trim(col("Level").cast("string")) == "")
)
df_issues.show(2)

#xxx need to test this filter
df_stage = df_stage.filter(
    (col("Status") == 0) &
    (col("Date").isNotNull()) &
    (trim(col("Date")) != "") &
    (col("Level").isNotNull()) &
    (trim(col("Level").cast("string")) != "")
)
df_stage.show(2)

#xxx need to convert from ISO currency code to HMC INT ID
#xxx also BB ticker to HMC GUID
#xxx and look up at HMC to get index Frequency (we only fill gaps for 8/Daily).  also get LEVEL/RETURN based and/or other metadata
df_stage = df_stage.select(
    col("SecurityIdentifier").alias("IndexId"),
    lit(8).alias("FrequencyId"),
    to_date(col("Date"), "yyyyMMdd").alias("AsOfDate"),
    col("Currency").alias("CurrencyId"),
    col("Level").alias("IndexLevel"),
    lit(0).alias("IndexReturn"),
    col("RunReason"),
    col("Guid"),
    col("AdditionalParam")
)

# Preview the result
df_stage.show(truncate=False)

#xxx df.write.mode("append").saveAsTable("STAGE_IndexReturnBloomberg")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, lead, datediff, lit, expr
from pyspark.sql.window import Window

# Load base tables as DataFrames (assume these are registered or read from Lakehouse)
# df_stage = spark.table("dbo.IndexReturn_STAGE")
# df_frequency = spark.table("dbo.Frequency")
# df_version = spark.table("util.Version")
# df_index = spark.table("dbo.Index")

# hardcode Daily: we only fill gaps for 8/Daily
frequency_id = 8 

# ------------------------------------------------------------------
# 3. Build CheckUniv
# ------------------------------------------------------------------
# if index_id_list:
#     index_ids = [id.strip() for id in index_id_list.split(",")]
#     df_check_univ = spark.createDataFrame(index_ids, "string").toDF("IDcolumn")
# else:
#     df_check_univ = (
#         df_stage.filter(
#             (col("IndexId").isNotNull()) &
#             (col("IsValid") == "Y") &
#             (col("Ignore") == "N") &
#             (expr("ISNULL(RunPurpose, '--')") != "NIGHTLY_INDEX_LOAD_SOIL")
#         )
#         .select("IndexId")
#         .distinct()
#         .withColumnRenamed("IndexId", "IDcolumn")
#     )

# ------------------------------------------------------------------
# 4. Build DateList
# ------------------------------------------------------------------
# df_joined = df_stage.alias("x").join(
#     df_check_univ.alias("cu"),
#     col("x.IndexId") == col("cu.IDcolumn"),
#     "inner"
# )

# this will cause grouping by the three columns and then ordered by the AsOfDate
window_spec = Window.partitionBy("IndexId", "FrequencyId", "CurrencyId").orderBy("AsOfDate")

# use "lead" to get value from "next" row knowing that we have sorted by AsOfDate 
df_date_list = (
    df_stage
    .filter(
        (col("FrequencyId") == frequency_id) # xxx todo: handle all frequencies
        #(col("IsValid") == "Y") &
        #(col("Ignore") == "N") 
        #(expr("ISNULL(x.RunPurpose, '--')") != "NIGHTLY_INDEX_LOAD_SOIL")
    )
    .withColumn("AsOfDateNext", lead("AsOfDate", 1).over(window_spec))
    .selectExpr(
        "IndexId", "CurrencyId", "FrequencyId", "AsOfDate", 
        "AsOfDateNext"
    )
)

df_date_list.show(10)

# Identify Gaps
df_gap = df_date_list.filter(
    datediff("AsOfDateNext", "AsOfDate") > 1
).withColumn("DaysDiff", datediff("AsOfDateNext", "AsOfDate")) \
 .withColumn("EventComment", lit(None).cast("string")) \
 .withColumn("ErrorType", lit("GAP"))

df_gap.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

should_fill_gap = 1
# ------------------------------------------------------------------
# 6. Stop here if ShouldFillGap = 0
# ------------------------------------------------------------------
if should_fill_gap != 1:
    print("Detected gaps but instructed not to fill. Returning result.")
    display(df_results)
    # Optionally return df_results or save it
else:
    # Explode gaps into missing dates (excluding endpoints)
    def date_range(start, end):
        return [str(d) for d in pd.date_range(start=start, end=end, freq='D')][1:-1]  # exclude start and end

    from pyspark.sql.functions import pandas_udf, explode, struct
    import pandas as pd
    from pyspark.sql.types import ArrayType, StringType

    @pandas_udf(ArrayType(StringType()))
    def generate_dates(start_series: pd.Series, end_series: pd.Series) -> pd.Series:
        return start_series.combine(end_series, date_range)

    gaps_with_dates = df_gap \
        .withColumn("MissingDates", generate_dates(col("AsOfDate"), col("AsOfDateNext"))) \
        .withColumn("MissingDate", explode(col("MissingDates")))

    # Load original stage again for reference IndexLevel
    #stage_ref = df_stage.filter((col("IsValid") == 'Y') & (col("Ignore") == 'N'))
    stage_ref = df_stage


gaps = gaps_with_dates.alias("gaps")
stage = stage_ref.alias("stage")

plug_rows = gaps \
    .join(stage, 
        (gaps_with_dates.IndexId == stage_ref.IndexId) &
        (gaps_with_dates.CurrencyId == stage_ref.CurrencyId) &
        (gaps_with_dates.FrequencyId == stage_ref.FrequencyId) &
        (gaps_with_dates.AsOfDate == stage_ref.AsOfDate),
        how='inner'
    ) \
    .select(
        col("gaps.IndexId"),
        col("gaps.FrequencyId"),
        col("MissingDate").cast("date").alias("AsOfDate"),
        col("gaps.CurrencyId"),
        col("IndexLevel"),
        lit(0).alias("IndexReturn"),
        col("RunReason"),
        col("Guid"),
        col("AdditionalParam")
        #lit("PLUG").alias("ReturnSource")
    )

plug_rows.show(20)
stage.show(20)

df_union = stage.union(plug_rows).orderBy("IndexId", "CurrencyId", "FrequencyId", "AsOfDate")

df_union.show(20)

# Write plug rows to the stage table
#plug_rows.write.format("delta").mode("append").saveAsTable("dbo.IndexReturn_STAGE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
