# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## IndexConstituent

# CELL ********************

"""
Logical Steps:
1. Set runtime parameters and constants (e.g., what steps to run, dates, debug).
2. Log the beginning of the process and insert an event log entry.
3. Determine the next version numbers for constituent and geography data.
4. If enabled, run the following processing steps:
   - Load raw index constituent data into staging.
   - Calculate and/or correct geographic region classifications.
   - Calculate and/or correct classification assignments.
   - Apply the final geographic and classification data to the warehouse.
5. Upon completion, update the event log with success status.
6. If an error occurs during any step, log the error and update the event log with failure status.

Notes:
- Calls to actual Spark SQL procedures are currently commented out and should be enabled when running in production.
- Logging and event log handling functions are stubbed/mocked and would be replaced with real implementations.
"""

from datetime import datetime

# PARAMETERS
ReuseVersion     = 1
indexIdList      = None 
LoadStartDate    = None 
LoadEndDate      = None
RunLoadProcess   = 1
RunCalcGeo       = 1
RunGeoCorrection = 1
RunCalcClass     = 1
RunClassCorrection = 1
RunApplyProcess  = 1
EventLogIdParent = None
EventLogId       = None
debug            = 0

# CONSTANTS
OBJECT_NAME = "etl.HMCStaging_DataWarehouse_IndexConstituent"
NOW = datetime.now()

# def log(message):
#     print(f"{OBJECT_NAME}: {message}")

# def insert_event_log(event_name, start_time, params): #loggingg
#     # Mocking event log insert
#     return 12345  # Dummy EventLogId

# def update_event_log(event_log_id, status, message): #logging
#     log(f"Updating EventLog {event_log_id} as {status}: {message}")

# BEGIN SCRIPT
log(f"Begin at {NOW}")
EventParms = f"{LoadStartDate or '<null>'}|{LoadEndDate or '<null>'}"
EventLogId = insert_event_log(OBJECT_NAME, NOW, EventParms)

# VERSION LOGIC
def get_next_version(load_name, stage, reuse, active):
    # Mock versioning logic
    return 101  # Dummy version number

try:
    VersionNumCon = get_next_version("IndexConst", "STAGE", ReuseVersion, "ACTIVE")
    VersionNumGeo = get_next_version("IndexGeo", "STAGE", ReuseVersion, "ACTIVE")

    # LOAD PROCESS
    if RunLoadProcess == 1:
        log("Starting constituent load...")
        # spark.sql("CALL etl.HMCStaging_DataWarehouse_IndexConstituent_Load(...)")
        log("IndexConstituent_Load complete.")
    else:
        log("Skipping constituent load.")

    # GEO CALCULATION
    if RunCalcGeo == 1 or RunGeoCorrection == 1:
        if RunCalcGeo == 1:
            log("Running GeoRegion calculation...")
            # spark.sql("CALL etl.HMCStaging_HMCStaging_IndexConstituent_CalcGeoRegion(...)")
        if RunGeoCorrection == 1:
            log("Running GeoRegion correction...")
            # spark.sql("CALL etl.HMCStaging_HMCStaging_IndexConstituent_CalcGeoCorrection(...)")
    else:
        print("Skipping Geo calculations.")

    # CLASSIFICATION CALCULATION
    if RunCalcClass == 1 or RunClassCorrection == 1:
        if RunCalcClass == 1:
            log("Running Classification calculation...")
            # spark.sql("CALL etl.HMCStaging_HMCStaging_IndexConstituent_CalcClassification(...)")
        if RunClassCorrection == 1:
            log("Running Classification correction...")
            # spark.sql("CALL etl.HMCStaging_HMCStaging_IndexConstituent_CalcClassCorrection(...)")
    else:
        print("Skipping Classification calculations.")

    # APPLY PROCESS
    if RunApplyProcess == 1:
        log("Running Apply process for Geo and Classification...")
        # spark.sql("CALL etl.HMCStaging_DataWarehouse_IndexGeography_Apply(...)")
        # spark.sql("CALL etl.HMCStaging_DataWarehouse_IndexClassification_ Apply(...)")
    else:
        print("Skipping Apply process.")

    # Finalize log
    # update_event_log(EventLogId, "SUCCESS", "Event completed successfully.")

except Exception as e:
    # log(f"Error occurred: {str(e)}")
    # update_event_log(EventLogId, "ERROR", str(e))
    raise e


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## IndexConstituent_Load

# CELL ********************

"""
ETL Script: Index Constituent Loader

This PySpark script loads and validates Index Constituent data from FactSet into staging, applies transformations,
and routes valid/invalid records into appropriate tables for downstream use or debugging.

Main Logic and Flow:

1. Parameter Initialization
   - Reads runtime parameters like date range, version, and thresholds from configuration or sets defaults.

2. Configuration Fetch
   - Pulls relevant values (start date, end date, max invalid countries) from a configuration Delta table.

3. Truncate Staging
   - Clears the staging table (`dbo.IndexConstituent_STAGE`) before reloading.

4. Raw Data Load
   - Loads raw FactSet data and joins with reference index data to filter out deleted indices.
   - Adds metadata fields like load timestamp, version number, and flags.

5. Country Matching
   - Joins with country reference table to resolve ISO country codes into internal `CountryId`.

6. AsOfDate Derivation
   - Converts `DataDate` to `AsOfDate` for timeline tracking and consistency.

7. Validation Logic
   - Flags rows with missing country, index ID, or `AsOfDate` as invalid.
   - Adds error messages to `LoadComment` for traceability.
   - Applies a threshold (`MaxInvalidCountry`) for allowable null countries per index.

8. Temp View for Country Count
   - Creates a temporary view to count how many null countries exist per index ID to support thresholding.

9. Re-validation
   - Adjusts the `IsValid` flag if the count of null country codes is within the allowed threshold.

10. Final Validations
    - Ensures all records have valid `IndexId` and `AsOfDate`.

11. Data Routing
    - Writes valid records into `dbo.IndexConstituent_PROCESSED`.
    - Writes invalid records into `dbo.IndexConstituent_ERROR`.

12. Logging
    - Basic start and end time logging for tracking execution in logs.

Note:
- All SQL `CALL` operations and logging functions like `insert_event_log` are assumed to be handled upstream or externally.
- This script is typically executed within a Spark environment (e.g., Synapse, Databricks, Fabric).

"""

from datetime import datetime, date
from pyspark.sql.functions import col, lit, when, expr, concat_ws, current_timestamp

# Parameters
indexIdList = None
LoadStartDate = None
LoadEndDate = None
VersionNum = None
EventLogIdParent = None
EventLogId = None  # OUTPUT param in SQL
MaxInvalidCountry = None

# Defaults and constants
OBJECT_NAME = 'etl.HMCStaging_DataWarehouse_IndexConstituent_Load'
NOW = datetime.now()
LOADNAME = 'IndexConst'
ACTIVE = 'ACTIVE'
STAGE = 'STAGE'

print(f"{OBJECT_NAME}: begin= {NOW}")

# Load configs from a config table (simulated via Delta table)
config_df = spark.read.table("util.config")

if LoadStartDate is None:
    LoadStartDate = config_df.filter((col("configName") == f"{LOADNAME}_LoadStartDate") &
                                     (col("startDate") <= lit(NOW)) &
                                     (col("endDate") >= lit(NOW))).select("configValue").first()
    LoadStartDate = LoadStartDate["configValue"] if LoadStartDate else date(2020, 1, 1)

if LoadEndDate is None:
    LoadEndDate = date.today()

if MaxInvalidCountry is None:
    MaxInvalidCountry = config_df.filter((col("configName") == f"{LOADNAME}_MaxInvalidCtry") &
                                         (col("startDate") <= lit(NOW)) &
                                         (col("endDate") >= lit(NOW))).select("configValue").first()
    MaxInvalidCountry = int(MaxInvalidCountry["configValue"]) if MaxInvalidCountry else 10

# Log Start
print(f"{OBJECT_NAME}: Starting load for VersionNum={VersionNum}, LoadStartDate={LoadStartDate}, LoadEndDate={LoadEndDate}")

# Truncate staging table
spark.sql("TRUNCATE TABLE dbo.IndexConstituent_STAGE")

# Load Raw Data
raw_df = spark.read.table("dbo.Factset_IndexConstituent_Const").dropDuplicates()
index_df = spark.read.table("HMCDataWarehouse.dbo.vwSourceReferenceIndex").filter(col("IndexStatus") != "Deleted")

stage_df = raw_df.join(index_df, raw_df["BENCHMARK_ID"] == index_df["IndexIdFactset"], how="left") \
    .withColumn("LoadType", lit(LOADNAME)) \
    .withColumn("LoadComment", when(col("IndexId").isNotNull(), lit("Index"))) \
    .withColumn("LoadProcess", lit(OBJECT_NAME)) \
    .withColumn("LoadTimestamp", lit(NOW)) \
    .withColumn("LoadStartDate", lit(LoadStartDate)) \
    .withColumn("EventLogId", lit(EventLogId)) \
    .withColumn("VersionNum", lit(VersionNum)) \
    .withColumn("IsValid", lit("Y")) \
    .withColumn("IsNew", lit("N")) \
    .withColumn("IsDup", lit("N")) \
    .withColumn("Ignore", lit("N"))

stage_df.select("*").write.mode("append").saveAsTable("dbo.IndexConstituent_STAGE")

# Country Matching
country_df = spark.read.table("HMCDataWarehouse.dbo.Country")
stage_df = spark.read.table("dbo.IndexConstituent_STAGE")

stage_df = stage_df.join(country_df, stage_df["Country"] == country_df["ISOCode2"], how="left") \
    .withColumn("CountryId", col("countryId")) \
    .withColumn("LoadComment", concat_ws(". ", col("LoadComment"), lit("Currency")))

# Update AsOfDate
stage_df = stage_df.withColumn("AsOfDate", expr("cast(DataDate as date)")) \
    .withColumn("LoadComment", concat_ws(". ", col("LoadComment"), lit("AsOfDate")))

# Flag invalid rows
stage_df = stage_df.withColumn("IsValid", when(col("CountryId").isNull(), lit("N")).otherwise(col("IsValid"))) \
    .withColumn("LoadComment", when(col("CountryId").isNull(), lit("ERROR: Country code not found.")).otherwise(col("LoadComment")))

stage_df.createOrReplaceTempView("stage")

spark.sql("DROP TABLE IF EXISTS #tempCountryCodeCount")
spark.sql("""
CREATE OR REPLACE TEMP VIEW tempCountryCodeCount AS
SELECT IndexId, SUM(CASE WHEN CountryId IS NULL THEN 1 ELSE 0 END) AS nullCountries
FROM stage
GROUP BY IndexId
""")

# Re-validate rows based on country threshold
stage_df = stage_df.alias("s").join(spark.table("tempCountryCodeCount").alias("t"), "IndexId") \
    .withColumn("IsValid", when((col("CountryId").isNull()) & (col("nullCountries") <= MaxInvalidCountry), lit("Y")).otherwise(col("IsValid")))

# Flag missing IndexId and AsOfDate
stage_df = stage_df.withColumn("IsValid", when(col("IndexId").isNull(), lit("N")).otherwise(col("IsValid"))) \
    .withColumn("LoadComment", when(col("IndexId").isNull(), lit("ERROR: Index not found.")).otherwise(col("LoadComment")))

stage_df = stage_df.withColumn("IsValid", when(col("AsOfDate").isNull(), lit("N")).otherwise(col("IsValid"))) \
    .withColumn("LoadComment", when(col("AsOfDate").isNull(), lit("ERROR: AsOfDate null.")).otherwise(col("LoadComment")))

# Write processed and error records
stage_df.filter((col("IsValid") == "Y") & (col("Ignore") == "N")) \
    .write.mode("append").saveAsTable("dbo.IndexConstituent_PROCESSED")

stage_df.filter((col("IsValid") == "N") | (col("Ignore") == "Y")) \
    .write.mode("append").saveAsTable("dbo.IndexConstituent_ERROR")

print(f"{OBJECT_NAME}: end = {datetime.now()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## IndexConstituent_CalcGeoRegion

# CELL ********************

"""
Logic Flow:
1. Retrieve or derive the version number if not supplied.
2. Identify invalid IndexConstituent records and exclude them from processing.
3. Insert valid IndexConstituent data into the IndexGeography_STAGE table with initial calculations.
4. Apply override values from the IndexGeography_Override table to the stage data.
5. Move any invalid records into the IndexGeography_ERROR table for auditing.
6. Move valid and complete records into the IndexGeography_PROCESSED table.
7. Log progress and counts throughout the process for traceability.

This version is designed to run in a Microsoft Fabric Lakehouse / PySpark notebook.
Ensure that referenced tables exist as managed Lakehouse tables or views.
"""
from pyspark.sql.functions import col, lit, when, sum as _sum, concat_ws, expr
from datetime import datetime

# Simulated parameters
indexIdList = None
LoadStartDate = None
LoadEndDate = None
VersionNum = None
isRunningSolo = 1
EventLogIdParent = None
debug = 0
now = datetime.now()

# Metadata constants
OBJECT_NAME = 'etl.HMCStaging_HMCStaging_IndexConstituent_CalcGeoRegion'
LOADNAME = 'IndexGeo'
USERID = (spark.sql("SELECT HMCUserId FROM HMCDataWarehouse.dbo.HMCUser WHERE DisplayName = 'SYSTEMACCOUNT'")
          .collect()[0]['HMCUserId'])

# Get version number if not provided
if VersionNum is None:
    VersionNum = (spark.sql("""
        SELECT VersionNum
        FROM HMCDataWarehouse.util.Version
        WHERE versionName = 'IndexGeo' AND versionType = 'STAGE'
    """).collect()[0]['VersionNum'])

# Log start of event (mock)
print(f"{OBJECT_NAME}: Begin at {now}")

# 1. Get list of invalid IndexIds to exclude

invalid_index_df = spark.sql("""
    SELECT DISTINCT(COALESCE(IndexId, '00000000-0000-0000-0000-000000000000')) AS IndexId
    FROM dbo.IndexConstituent_STAGE
    WHERE IsValid = 'N'
""")
invalid_index_df.createOrReplaceTempView("excludeIndex")


# 2. Insert Valid Data into dbo.IndexGeography_STAGE

spark.sql("TRUNCATE TABLE dbo.IndexGeography_STAGE")

valid_data_df = spark.sql("""
    SELECT ic.IndexId, cr.GeographicStrategyId, ic.AsOfDate,
           SUM(ic.Weight) AS ExposurePct,
           0 AS CorrectionPct,
           current_timestamp() AS UpdateTimeStamp,
           '{USERID}' AS UpdateByHMCUserId,
           '{LOADNAME}' AS LoadType,
           NULL AS LoadComment,
           '{OBJECT_NAME}' AS LoadProcess,
           current_timestamp() AS LoadTimestamp,
           NULL AS LoadStartDate,
           NULL AS EventLogId,
           {VersionNum} AS VersionNum,
           'Y' AS IsValid,
           'N' AS IsNew,
           'N' AS IsDup,
           'N' AS Ignore,
           NULL AS ExposurePct_Override
    FROM dbo.IndexConstituent_STAGE ic
    JOIN HMCDataWarehouse.dbo.CountryRegion cr ON ic.CountryId = cr.CountryId
    WHERE ic.IndexId NOT IN (SELECT IndexId FROM excludeIndex)
    GROUP BY ic.IndexId, ic.AsOfDate, cr.GeographicStrategyId
""")

valid_data_df.write.mode("append").saveAsTable("dbo.IndexGeography_STAGE")
print(f"{OBJECT_NAME}: Inserted {valid_data_df.count()} rows to IndexGeography_STAGE")


# 3. Apply Overrides

overrides_df = spark.table("etl.IndexGeography_Override")
stage_df = spark.table("dbo.IndexGeography_STAGE")

updated_stage_df = stage_df.alias("stage") \
    .join(overrides_df.alias("igo"), 
          (col("stage.IndexId") == col("igo.IndexIdWarehouse")) &
          (col("stage.GeographicStrategyId") == col("igo.GeographicStrategyId")), "left") \
    .withColumn("ExposurePct_Override", 
                when(col("stage.ExposurePct_Override").isNull(), col("igo.ExposurePct_Override"))
                .otherwise(col("stage.ExposurePct_Override"))) \
    .withColumn("CorrectionPct_Override", lit(0.00)) \
    .withColumn("LoadComment", 
                when(col("igo.ExposurePct_Override").isNotNull(),
                     concat_ws('. ', lit("Exposure OVERRIDE"), col("stage.LoadComment")))
                .otherwise(col("stage.LoadComment")))

updated_stage_df.write.mode("overwrite").saveAsTable("dbo.IndexGeography_STAGE")
print(f"{OBJECT_NAME}: Overrides applied")

# 4. Move invalid rows to ERROR table

invalid_rows = spark.sql("""
    SELECT ic.IndexId,
           'ERROR: Invalid constituent data./' + COALESCE(ic.LoadComment, '') AS LoadComment,
           current_timestamp() AS LoadTimestamp
    FROM dbo.IndexConstituent_STAGE ic
    JOIN excludeIndex ei ON ic.IndexId = ei.IndexId
""")

invalid_rows.write.mode("append").saveAsTable("dbo.IndexGeography_ERROR")


# 5. Move good rows to PROCESSED table

good_rows = spark.sql("""
    SELECT *
    FROM dbo.IndexGeography_STAGE
    WHERE IsValid = 'Y' AND Ignore = 'N'
""")

good_rows.write.mode("append").saveAsTable("dbo.IndexGeography_PROCESSED")
print(f"{OBJECT_NAME}: Processed rows moved to PROCESSED table")


# 6. Finish event log (mock)
print(f"{OBJECT_NAME}: End at {datetime.now()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## IndexConstituent_CalcGeoCorrection

# CELL ********************

"""
Logic Flow:
1. Initialization of parameters and logging
2. Derive VersionNum and LoadStartDate if not provided
3. Create a correction factor table by summing exposure percentages
4. Reset previous correction values and remove placeholder data
5. Apply correction values to the highest exposure rows
6. Insert placeholder rows for missing regions
7. Log the completion event
"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Initialization
object_name = 'etl.HMCStaging_HMCStaging_IndexConstituent_CalcGeoCorrection'
now = datetime.now()
print(f"{object_name}: begin= {now}.")

# Simulated parameter inputs
indexIdList = None
LoadStartDate = None
LoadEndDate = None
VersionNum = None
isRunningSolo = 0
EventLogIdParent = None
EventLogId = None
debug = 0

LOADNAME = 'IndexGeo'
ACTIVE = 'ACTIVE'
STAGE = 'STAGE'

# Simulated fetch of SYSTEMACCOUNT user ID
USERID = spark.sql("SELECT HMCUserId FROM HMCUser WHERE DisplayName = 'SYSTEMACCOUNT'").first()[0]

if VersionNum is None:
    VersionNum = spark.sql(f"""
        SELECT VersionNum
        FROM util.Version
        WHERE versionName = '{LOADNAME}' AND versionType = '{STAGE}'
    """).first()[0]

if LoadStartDate is None:
    result = spark.sql(f"""
        SELECT configValue
        FROM util.config
        WHERE configName = '{LOADNAME}_LoadStartDate'
        AND startDate <= current_date()
        AND endDate >= current_date()
    """).first()
    LoadStartDate = result[0] if result else '2020-01-01'

if LoadEndDate is None:
    LoadEndDate = datetime.today().date()

print(f"{object_name}: @LoadStartDate = {LoadStartDate}, @LoadEndDate = {LoadEndDate}, @VersionNum = {VersionNum}")

# Index correction calculation
stage = spark.table("dbo.IndexGeography_STAGE") \
    .filter((F.col("AsOfDate") >= F.lit(LoadStartDate)))

# Inner aggregation
agg_stage = stage.groupBy("indexid", "GeographicStrategyid", "AsOfDate") \
                 .agg(F.sum("exposurepct").alias("exposurepct"))

# Outer aggregation for correction
correction_df = agg_stage.groupBy("indexid", "AsOfDate") \
                         .agg((100 - F.sum("exposurepct")).alias("CorrectionPct"))
correction_df.createOrReplaceTempView("IndexCorrection")

# Reset CorrectionPct to 0
stage = stage.alias("stage").join(
    correction_df.alias("correct"),
    (F.col("stage.indexid") == F.col("correct.indexid")) &
    (F.col("stage.AsOfDate") == F.col("correct.AsOfDate")),
    "inner"
).withColumn("CorrectionPct", F.lit(0))

# Delete old "PLUG. missing region" rows
stage_filtered = spark.table("dbo.IndexGeography_STAGE") \
    .filter(
        (F.col("AsOfDate") >= F.lit(LoadStartDate)) &
        (F.col("AsOfDate") <= F.lit(LoadEndDate)) &
        (F.col("LoadComment").contains("PLUG. missing region"))
    )
# stage_filtered can be used to delete from the source table (depends on your platform's support for deletes)

# Re-apply CorrectionPct
windowSpec = Window.partitionBy("IndexId", "AsOfDate").orderBy(F.desc("ExposurePct"))

ranked = stage.withColumn("exposurerank", F.row_number().over(windowSpec)) \
              .filter(F.col("exposurerank") == 1)

stage_corrected = stage.join(
    correction_df,
    ["indexid", "AsOfDate"]
).join(
    ranked.select("indexid", "GeographicStrategyId", "AsOfDate"),
    ["indexid", "GeographicStrategyId", "AsOfDate"]
).withColumn("CorrectionPct", F.col("CorrectionPct"))

# Fill in missing regions
geo_strategies = spark.table("GeographicStrategy")
unique_pairs = spark.table("dbo.IndexGeography_STAGE") \
                    .filter((F.col("AsOfDate") >= F.lit(LoadStartDate)) &
                            (F.col("AsOfDate") <= F.lit(LoadEndDate))) \
                    .select("indexid", "AsOfDate").distinct()

missing_region = geo_strategies.crossJoin(unique_pairs)

existing = spark.table("dbo.IndexGeography_STAGE") \
                .filter((F.col("AsOfDate") >= F.lit(LoadStartDate)) &
                        (F.col("AsOfDate") <= F.lit(LoadEndDate))) \
                .select("indexid", "GeographicStrategyId", "AsOfDate")

missing_to_insert = missing_region.join(
    existing,
    ["indexid", "GeographicStrategyId", "AsOfDate"],
    "leftanti"
).withColumn("ExposurePct", F.lit(0.0)) \
 .withColumn("CorrectionPct", F.lit(0.0)) \
 .withColumn("LoadType", F.lit(LOADNAME)) \
 .withColumn("LoadComment", F.lit("PLUG. missing region")) \
 .withColumn("LoadProcess", F.lit(object_name)) \
 .withColumn("LoadTimeStamp", F.lit(now)) \
 .withColumn("LoadStartDate", F.lit(LoadStartDate)) \
 .withColumn("EventLogId", F.lit(EventLogId)) \
 .withColumn("VersionNum", F.lit(VersionNum)) \
 .withColumn("IsValid", F.lit("Y")) \
 .withColumn("IsNew", F.lit("N")) \
 .withColumn("IsDup", F.lit("N")) \
 .withColumn("Ignore", F.lit("N")) \
 .withColumn("UpdateTimeStamp", F.lit(now)) \
 .withColumn("UpdateByHMCUserId", F.lit(USERID))

# missing_to_insert.write.insertInto("dbo.IndexGeography_STAGE", overwrite=False)

# Final logging and cleanup
print(f"{object_name}: end  = {datetime.now()}.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## IndexConstituent_CalcClassification

# CELL ********************

"""
Logic Flow:
1. Initialize parameters and environment (e.g., load metadata, user ID)
2. Identify invalid index constituents (IsValid = 'N') to exclude from staging
3. Truncate the staging table (IndexClassification_STAGE)
4. Insert valid aggregated classification data into IndexClassification_STAGE
5. Perform data quality checks (e.g., SectorName NULL)
6. Move bad rows to IndexClassification_ERROR
7. Move valid rows to IndexClassification_PROCESSED
8. Log event end and cleanup
"""

from pyspark.sql import functions as F
from datetime import datetime

# Parameters (replace these with actual inputs as needed)
index_id_list = None
load_start_date = None
load_end_date = None
version_num = None
is_running_solo = 1
event_log_id_parent = None
event_log_id = None
debug = 0

object_name = "etl.HMCStaging_HMCStaging_IndexConstituent_CalcClassification"
now = datetime.now()

# Load supporting metadata
user_id = spark.sql("""
    SELECT HMCUserId 
    FROM HMCDataWarehouse.dbo.HMCUser 
    WHERE DisplayName = 'SYSTEMACCOUNT'
""").collect()[0][0]

if version_num is None:
    version_num = spark.sql("""
        SELECT VersionNum 
        FROM HMCDataWarehouse.util.Version 
        WHERE VersionName = 'IndexGeo' AND VersionType = 'STAGE'
    """).collect()[0][0]

# Log event start (you may replace this with a logging function)
event_parms = f"{load_start_date}|{load_end_date}|{is_running_solo}"
# (Simulate event logging)
event_log_id = 99999  # Placeholder

# Step 1: Identify invalid index constituents
exclude_index_df = spark.sql("""
    SELECT DISTINCT COALESCE(IndexId, '00000000-0000-0000-0000-000000000000') AS IndexId
    FROM dbo.IndexConstituent_STAGE 
    WHERE IsValid = 'N'
""")
exclude_index_df.createOrReplaceTempView("excludeIndex")

# Step 2: Truncate the staging table
spark.sql("TRUNCATE TABLE dbo.IndexClassification_STAGE")

# Step 3: Insert valid data into staging
valid_classification_df = spark.sql("""
    SELECT 
        ic.IndexId, ic.AsOfDate, 
        ic.SectorCode, ic.SectorName,
        ic.IndustryCode, ic.IndustryName,
        ic.IndustryGrpCode, ic.IndustryGrpName,
        ic.SubIndustryCode, ic.SubIndustryName,
        SUM(COALESCE(ic.Weight, 0)) AS ClassificationPct,
        0 AS CorrectionPct,
        CURRENT_TIMESTAMP() AS UpdateTimeStamp,
        '{user_id}' AS UpdateByHMCUserId,
        'IndexGeo' AS LoadType,
        NULL AS LoadComment,
        '{object_name}' AS LoadProcess,
        CURRENT_TIMESTAMP() AS LoadTimestamp,
        DATE('{load_start_date}') AS LoadStartDate,
        {event_log_id} AS EventLogId,
        {version_num} AS VersionNum,
        'Y' AS IsValid, 'N' AS IsNew, 'N' AS IsDup, 'N' AS Ignore
    FROM dbo.IndexConstituent_STAGE ic
    LEFT ANTI JOIN excludeIndex x ON COALESCE(ic.IndexId, '00000000-0000-0000-0000-000000000000') = x.IndexId
    GROUP BY 
        ic.IndexId, ic.AsOfDate, ic.SectorCode, ic.SectorName,
        ic.IndustryCode, ic.IndustryName,
        ic.IndustryGrpCode, ic.IndustryGrpName,
        ic.SubIndustryCode, ic.SubIndustryName
""")
valid_classification_df.write.mode("append").saveAsTable("dbo.IndexClassification_STAGE")

# Step 4: Data Quality Check - Null SectorName
spark.sql("""
    UPDATE dbo.IndexClassification_STAGE 
    SET 
        IsValid = 'N',
        LoadComment = CONCAT('ERROR: Sector NULL', 
                             CASE WHEN LoadComment IS NOT NULL THEN '. ' || LoadComment ELSE '' END)
    WHERE SectorName IS NULL AND IsValid = 'Y' AND Ignore = 'N'
""")

# Step 5: Move invalid rows to ERROR table
spark.sql("""
    INSERT INTO dbo.IndexClassification_ERROR
    SELECT 
        ic.IndexId, ic.IndexIdentifier, ic.SecurityIdentifier,
        ic.AsOfDate, ic.SectorCode, ic.SectorName,
        ic.IndustryCode, ic.IndustryName,
        ic.IndustryGrpCode, ic.IndustryGrpName,
        ic.SubIndustryCode, ic.SubIndustryName,
        NULL AS ClassificationPct, NULL AS CorrectionPct,
        CURRENT_TIMESTAMP(), '{object_name}' AS UpdateByHMCUserId,
        ic.LoadType,
        CONCAT('ERROR: Invalid constituent data./', 
               CASE WHEN ic.LoadComment IS NOT NULL THEN '. ' || ic.LoadComment ELSE '' END),
        ic.LoadComment,
        ic.LoadProcess, CURRENT_TIMESTAMP(), ic.LoadStartDate, ic.EventLogId, ic.VersionNum,
        ic.IsValid, ic.IsNew, ic.IsDup, ic.Ignore
    FROM dbo.IndexConstituent_STAGE ic
    JOIN excludeIndex x ON COALESCE(ic.IndexId, '00000000-0000-0000-0000-000000000000') = x.IndexId
""")

# Step 6: Move valid rows to PROCESSED table
spark.sql("""
    INSERT INTO dbo.IndexClassification_PROCESSED
    SELECT *
    FROM dbo.IndexClassification_STAGE
    WHERE IsValid = 'Y' AND Ignore = 'N'
""")

# Step 7: Event logging end (simulated)
# (Would normally call `util.Update_EventLog`)
print(f"{object_name}: Load Complete.")

# Step 8: Cleanup (dropping temp views, optional in Spark depending on use case)
spark.catalog.dropTempView("excludeIndex")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## IndexConstituent_CalcClassCorrection

# CELL ********************

# -----------------------------------------------------------------------------
# Logic Flow:
# 1. Initialize parameters and defaults (dates, user, version)
# 2. Log event start
# 3. Create a temporary view of correction percentages per index and as-of date
# 4. Reset prior correction values in IndexClassification_STAGE to zero
# 5. Set new correction values in IndexClassification_STAGE for the max classification
# 6. Log event end and clean up
# -----------------------------------------------------------------------------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

# Parameters (can be replaced with function arguments)
index_id_list = None
load_start_date = None
load_end_date = None
version_num = None
is_running_solo = 1
event_log_id_parent = None
event_log_id = None
debug = 0

object_name = 'etl.HMCStaging_HMCStaging_IndexConstituent_CalcClassCorrection'
now = datetime.now()
load_name = 'IndexGeo'
user_id = spark.sql("""
    SELECT HMCUserId 
    FROM HMCDataWarehouse.dbo.HMCUser 
    WHERE DisplayName = 'SYSTEMACCOUNT'
""").collect()[0][0]

if version_num is None:
    version_num = spark.sql("""
        SELECT VersionNum 
        FROM HMCDataWarehouse.util.Version 
        WHERE VersionName = 'IndexGeo' AND VersionType = 'STAGE'
    """).collect()[0][0]

if load_start_date is None:
    result = spark.sql(f"""
        SELECT configValue 
        FROM util.config 
        WHERE configName = '{load_name}_LoadStartDate'
          AND startDate <= current_date()
          AND endDate >= current_date()
    """).collect()
    if result:
        load_start_date = result[0][0]
    else:
        load_start_date = '2020-01-01'

if load_end_date is None:
    load_end_date = now.date()

# Simulated event log insert (replace with real logging if needed)
event_parms = f"{load_start_date}|{load_end_date}|{is_running_solo}"
event_log_id = 99998  # placeholder ID

# -----------------------------------------------------------------------------
# Step 3: Calculate Correction Percentages
# -----------------------------------------------------------------------------
index_stage = spark.table("dbo.IndexClassification_STAGE") \
    .filter(
        (F.col("AsOfDate") >= F.lit(load_start_date)) &
        (F.col("AsOfDate") <= F.lit(load_end_date)) &
        (F.col("IsValid") == 'Y')
    )

grouped = index_stage.groupBy(
    "IndexId", "SectorCode", "SectorName", "IndustryCode", "IndustryName",
    "IndustryGrpCode", "IndustryGrpName", "SubIndustryCode", "SubIndustryName", "AsOfDate"
).agg(F.sum(F.coalesce("ClassificationPct", F.lit(0))).alias("ClassificationPct"))

correction_df = grouped.groupBy("IndexId", "AsOfDate") \
    .agg((F.lit(100) - F.sum("ClassificationPct")).alias("CorrectionPct"))

correction_df.createOrReplaceTempView("IndexCorrection")

print(f"{object_name}: rows inserted into IndexCorrection = {correction_df.count()}")

# -----------------------------------------------------------------------------
# Step 4: Reset old correction to 0
# -----------------------------------------------------------------------------
reset_df = index_stage.alias("stage").join(
    correction_df.alias("correct"),
    (F.col("stage.IndexId") == F.col("correct.IndexId")) &
    (F.col("stage.AsOfDate") == F.col("correct.AsOfDate"))
).filter(F.col("stage.CorrectionPct") != 0)

reset_df = reset_df.withColumn("CorrectionPct", F.lit(0))
reset_df.select("stage.*").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.IndexClassification_STAGE")

print(f"{object_name}: rows reset CorrectionPct to 0 = {reset_df.count()}")

# -----------------------------------------------------------------------------
# Step 5: Update CorrectionPct only for highest ClassificationPct per group
# -----------------------------------------------------------------------------
window_spec = Window.partitionBy("IndexId", "AsOfDate").orderBy(F.desc("ClassificationPct"))
ranked_df = index_stage \
    .join(correction_df, ["IndexId", "AsOfDate"]) \
    .withColumn("ClassRank", F.row_number().over(window_spec)) \
    .filter(F.col("ClassRank") == 1) \
    .select("IndexId", "AsOfDate", "SectorName", "IndustryName", "CorrectionPct")

updated_df = index_stage.alias("stage") \
    .join(ranked_df.alias("maxPct"), 
          (F.col("stage.IndexId") == F.col("maxPct.IndexId")) &
          (F.col("stage.AsOfDate") == F.col("maxPct.AsOfDate")) &
          (F.col("stage.SectorName") == F.col("maxPct.SectorName")) &
          (F.col("stage.IndustryName") == F.col("maxPct.IndustryName"))) \
    .join(correction_df.alias("correct"),
          (F.col("stage.IndexId") == F.col("correct.IndexId")) &
          (F.col("stage.AsOfDate") == F.col("correct.AsOfDate"))) \
    .filter(F.col("correct.CorrectionPct") != 0) \
    .withColumn("CorrectionPct", F.col("correct.CorrectionPct"))

updated_df.select("stage.*").write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.IndexClassification_STAGE")

print(f"{object_name}: rows updated CorrectionPct = {updated_df.count()}")

# -----------------------------------------------------------------------------
# Step 6: Log event end
# -----------------------------------------------------------------------------
end_time = datetime.now()
event_message = "Load Complete."

# Simulated event log update
print(f"{object_name}: end = {end_time}. Event message: {event_message}")

# -----------------------------------------------------------------------------
# Cleanup
# -----------------------------------------------------------------------------
spark.catalog.dropTempView("IndexCorrection")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## IndexConstituent_IndexGeography_Apply

# CELL ********************



# =====================================================================================
# Logic Flow:
# 1. Initialize parameters, metadata, and context
# 2. Log event start into event log system
# 3. Load and filter staging data from IndexGeography_STAGE
# 4. Load target data from IndexGeography table in HMCDataWarehouse
# 5. Identify new records to insert and changed records to update
# 6. Simulate insert and update operations
# 7. Log event end and rows affected
# 8. Perform cleanup (if necessary)
# =====================================================================================

# 1. Initialize parameters and metadata
OBJECT_NAME = "etl.HMCStaging_DataWarehouse_IndexGeography_Apply"
NOW = datetime.now()
LOADNAME = "IndexGeo"
HMCDataWarehouse = "HMCDataWarehouse"
CrimsonX = "CrimsonX"

# These would be dynamically fetched in a real pipeline
USERID = "SYSTEMACCOUNT_ID"          # e.g., lookup from HMCUser
FIRMOBJECTTYPEID = 101               # e.g., lookup from HMCObjectType
CRIMSONXSOURCESYSTEMID = 202         # e.g., lookup from HMCObjectSourceSystem

# Input parameters (usually passed into a function or notebook context)
indexIdList = None
LoadStartDate = None
LoadEndDate = None
VersionNum = 1
EventLogIdParent = None
EventLogId = None

# 2. Log event start
EventParms = f"{VersionNum or '<null>'}|{LoadStartDate or '<null>'}|{LoadEndDate or '<null>'}"
EventLogId = log_event_start(
    EventName=OBJECT_NAME,
    EventType="ETL",
    EventParms=EventParms,
    EventStart=NOW,
    EventMessage="Event Start.",
    EventLogIdParent=EventLogIdParent
)

# 3. Load and filter staging data
index_geo_stage_df = spark.table("dbo.IndexGeography_STAGE") \
    .filter((F.col("IsValid") == "Y") & (F.col("Ignore") == "N")) \
    .select(
        "IndexId", "GeographicStrategyId", "AsOfDate",
        F.coalesce("ExposurePct_Override", "ExposurePct").alias("ExposurePct"),
        F.coalesce("CorrectionPct_Override", "CorrectionPct").alias("CorrectionPct")
    ) \
    .withColumn("VersionNum", F.lit(VersionNum)) \
    .withColumn("UpdateTimeStamp", F.lit(NOW)) \
    .withColumn("UpdateByHMCUserId", F.lit(USERID)) \
    .dropDuplicates(["IndexId", "GeographicStrategyId", "AsOfDate"])

# 4. Load existing target data
target_df = spark.table("HMCDataWarehouse.dbo.IndexGeography")

# 5. Identify records to insert or update
# Insert: new records not present in the target
insert_df = index_geo_stage_df.alias("src") \
    .join(target_df.alias("tgt"),
          on=["IndexId", "GeographicStrategyId", "AsOfDate"],
          how="left_anti")

# Update: records with different ExposurePct or CorrectionPct
update_df = index_geo_stage_df.alias("src") \
    .join(target_df.alias("tgt"),
          on=["IndexId", "GeographicStrategyId", "AsOfDate"],
          how="inner") \
    .filter(
        (F.coalesce(F.col("tgt.ExposurePct"), F.lit(-98765)) != F.col("src.ExposurePct")) |
        (F.coalesce(F.col("tgt.CorrectionPct"), F.lit(-98765)) != F.col("src.CorrectionPct"))
    )

# 6. Simulate insert and update operations (pseudo-actions)
insert_count = insert_df.count()
update_count = update_df.count()
rows_affected = insert_count + update_count

# NOTE: Replace below with actual write/merge logic if needed
# insert_df.write.format("delta").mode("append").saveAsTable("HMCDataWarehouse.dbo.IndexGeography")
# merge_to_target(update_df, "HMCDataWarehouse.dbo.IndexGeography", keys=["IndexId", "GeographicStrategyId", "AsOfDate"])

# 7. Log event end
EventMessage = f"Load Complete. version={VersionNum or '<null>'}"
log_event_end(
    EventLogId=EventLogId,
    EventEnd=datetime.now(),
    EventEndCondition="Success",
    EventMessage=EventMessage,
    RowsAffected=rows_affected
)

# 8. Cleanup
# No temporary tables used


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## IndexConstituent_IndexClassification_Apply

# CELL ********************

from pyspark.sql import functions as F
from datetime import datetime

# =====================================================================================
# Logic Flow:
# 1. Initialize parameters, metadata, and context
# 2. Log event start into event log system
# 3. Create distinct staging DataFrame (#IndexList equivalent)
# 4. Delete records in target table (IndexIndustry) that are not present in staging data
# 5. Merge staging data into target table:
#    - Insert new records not in target
#    - Update records with changed values
# 6. Log event end with number of rows affected
# 7. Cleanup temporary resources (if any)
# =====================================================================================

# 1. Initialize parameters and metadata
OBJECT_NAME = "etl.HMCStaging_DataWarehouse_IndexClassification_Apply"
NOW = datetime.now()

LOADNAME = "IndexClass"
HMCDataWarehouse = "HMCDataWarehouse"
USERID = "SYSTEMACCOUNT_ID"  # Should be fetched from HMCUser table
VersionNum = 1               # To be passed as parameter or set dynamically

# 2. Log event start (pseudo-function, replace with actual logging)
EventParms = f"{VersionNum or '<null>'}|<LoadStartDate>|<LoadEndDate>"
EventLogId = log_event_start(
    EventName=OBJECT_NAME,
    EventType="ETL",
    EventParms=EventParms,
    EventStart=NOW,
    EventMessage="Event Start."
)

# 3. Create staging distinct dataset (equivalent to #IndexList temp table)
index_class_stage_df = spark.table("dbo.IndexClassification_STAGE") \
    .filter((F.col("IsValid") == "Y") & (F.col("Ignore") == "N")) \
    .select(
        "IndexId", "AsOfDate", "SectorName", "IndustryName",
        "IndustryGrpName", "SubIndustryName"
    ).distinct()

# 4. Load target IndexIndustry data
index_industry_df = spark.table(f"{HMCDataWarehouse}.dbo.IndexIndustry")

# 5. Delete rows in target that are not in staging (simulate DELETE with anti-join)
# Find rows in target matching staging index and AsOfDate
join_condition = [
    index_industry_df.IndexId == index_class_stage_df.IndexId,
    index_industry_df.AsOfDate == index_class_stage_df.AsOfDate
]
# Rows in target to keep: those matching staging rows with all classification fields matching
keep_condition = (
    (index_industry_df.SectorName == index_class_stage_df.SectorName) &
    (index_industry_df.IndustryName == index_class_stage_df.IndustryName) &
    (index_industry_df.IndustryGrpName == index_class_stage_df.IndustryGrpName) &
    (index_industry_df.SubIndustryName == index_class_stage_df.SubIndustryName)
)

# Rows in target not existing in staging with same classification => to delete
to_delete_df = index_industry_df.alias("wh") \
    .join(index_class_stage_df.alias("x"), on=["IndexId", "AsOfDate"], how="inner") \
    .join(index_class_stage_df.alias("stage"),
          on=[
            (index_industry_df.IndexId == F.col("stage.IndexId")),
            (index_industry_df.AsOfDate == F.col("stage.AsOfDate")),
            (index_industry_df.SectorName == F.col("stage.SectorName")),
            (index_industry_df.IndustryName == F.col("stage.IndustryName")),
            (index_industry_df.IndustryGrpName == F.col("stage.IndustryGrpName")),
            (index_industry_df.SubIndustryName == F.col("stage.SubIndustryName")),
          ], how="leftanti")

rows_deleted = to_delete_df.count()
# TODO: Perform actual delete from target table in real implementation

print(f"{OBJECT_NAME}: rows deleted(HMCDataWarehouse.dbo.IndexIndustry)= {rows_deleted}")

# 6. Prepare staging source data for merge with all required columns
source_df = spark.table("dbo.IndexClassification_STAGE") \
    .filter((F.col("IsValid") == "Y") & (F.col("Ignore") == "N")) \
    .select(
        "IndexId", "SectorName", "IndustryName",
        "SectorCode", "IndustryCode",
        "IndustryGrpCode", "IndustryGrpName", "SubIndustryCode", "SubIndustryName",
        "AsOfDate", "ClassificationPct", "CorrectionPct"
    ).withColumn("VersionNum", F.lit(VersionNum)) \
     .withColumn("UpdateTimeStamp", F.lit(NOW)) \
     .withColumn("UpdateByHMCUserId", F.lit(OBJECT_NAME)) \
     .distinct()

# 7. Merge logic: Identify inserts and updates
join_cols = ["IndexId", "SectorName", "IndustryGrpName", "IndustryName", "SubIndustryName", "AsOfDate"]

# Insert new records not in target
insert_df = source_df.alias("src").join(index_industry_df.alias("tgt"),
                                        on=join_cols, how="left_anti")

# Update records where values differ
update_condition = (
    (F.coalesce(F.col("tgt.ClassificationPct"), F.lit(-98765)) != F.col("src.ClassificationPct")) |
    (F.coalesce(F.col("tgt.CorrectionPct"), F.lit(-98765)) != F.col("src.CorrectionPct")) |
    (F.coalesce(F.col("tgt.SectorCode"), F.lit(-99)) != F.col("src.SectorCode")) |
    (F.coalesce(F.col("tgt.IndustryGrpCode"), F.lit(-99)) != F.col("src.IndustryGrpCode")) |
    (F.coalesce(F.col("tgt.IndustryCode"), F.lit(-99)) != F.col("src.IndustryCode")) |
    (F.coalesce(F.col("tgt.SubIndustryCode"), F.lit(-99)) != F.col("src.SubIndustryCode"))
)

update_df = source_df.alias("src").join(index_industry_df.alias("tgt"),
                                       on=join_cols, how="inner") \
    .filter(update_condition)

rows_inserted = insert_df.count()
rows_updated = update_df.count()
rows_affected = rows_inserted + rows_updated

# TODO: Implement actual insert and update to target table (e.g., using Delta Lake merge or JDBC upsert)

print(f"{OBJECT_NAME}: rows merged(HMCDataWarehouse.dbo.IndexIndustry)= {rows_affected}")

# 8. Log event end
EventMessage = f"Load Complete. version={VersionNum or '<null>'}"
log_event_end(
    EventLogId=EventLogId,
    EventEnd=datetime.now(),
    EventEndCondition="Success",
    EventMessage=EventMessage,
    RowsAffected=rows_affected
)

# 9. Cleanup (no temp tables needed in PySpark)


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

from pyspark.sql.functions import col, concat_ws
import requests
import json
import os

secret_value = mssparkutils.credentials.getSecret('https://kv-dc1dev-adf.vault.azure.net/', 'BearerTokenAPI')

# ------------------------------------------
# 0. Fetch JSON from KnowBe4 API and save
# ------------------------------------------

# API config
base_url = "https://us.api.knowbe4.com/v1/users"
bearer_token = secret_value

headers = {
    "Authorization": f"Bearer {bearer_token}",
    "Accept": "application/json"
}
params = {
    "page": 1,
    "per_page": 500
}

all_users = []
while True:
    response = requests.get(base_url, headers=headers, params=params)
    response.raise_for_status()

    users_page = response.json()
    if not users_page:
        break

    print(f"Fetched {len(users_page)} users on page {params['page']}")
    for user in users_page:
        print(user)  # Print each user

    all_users.extend(users_page)

    if len(users_page) < params["per_page"]:
        break  # No more pages
    params["page"] += 1



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
