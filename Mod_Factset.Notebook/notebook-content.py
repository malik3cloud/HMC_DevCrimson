# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e9fc4e80-ff69-4d45-bbdd-892592889465",
# META       "default_lakehouse_name": "lh_curated",
# META       "default_lakehouse_workspace_id": "33535eb8-4d07-49bc-b3a5-cc91d3aa6ced",
# META       "known_lakehouses": [
# META         {
# META           "id": "e9fc4e80-ff69-4d45-bbdd-892592889465"
# META         },
# META         {
# META           "id": "13ef97da-5da2-466d-8c5f-2a70572c6558"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

"""
Dev Notes (Just things to note)
This is incremental load
Print all records where the BENCHMARK_ID was not mapped to Crimson IndexId - this is important to business
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Import required libraries
from datetime import datetime, date
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, when, expr, concat_ws, current_timestamp, lower
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Optional, List, Dict, Any, Union

spark.conf.set("spark.sql.caseSensitive","true")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
spark.conf.set("spark.sql.analyzer.maxIterations", 1000)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run factset_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(test_utils())
# Removed logging
# Removed user ID logging
# Removed incremental load logic, this will be handles differently will be added once this code is complete

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# PARAMETERS - These can be overridden by notebook parameters
# Option to run only parts
ReuseVersion     = 1
indexIdList      = None 
LoadStartDate    = None 
LoadEndDate      = None
RunLoadProcess   = 1
RunCalcGeo       = 1
RunGeoCorrection = 1
RunCalcClass     = 1
RunClassCorrection = 1
RunApplyProcess  = 1 # -> are these always 1 or are they dependent somewhere else ?
EventLogIdParent = None
EventLogId       = None
debug            = 0
MaxInvalidCountry = 10

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Build this from standard nbs
bronze_lh_id = 'abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/13ef97da-5da2-466d-8c5f-2a70572c6558'
silver_lh_id = 'abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/e9fc4e80-ff69-4d45-bbdd-892592889465'
ic_stage_path = f"{bronze_lh_id}/Files/IndexConstituent_STAGE.parquet"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get Spark session
spark = SparkSession.builder.getOrCreate()

# Initialize main process metadata
OBJECT_NAME = "etl.HMCStaging_DataWarehouse_IndexConstituent"
# metadata = initialize_metadata(
#     spark=spark,
#     object_name=OBJECT_NAME,
#     load_name="IndexConst",
    # load_start_date=LoadStartDate,
    # load_end_date=LoadEndDate,
    # event_log_id=EventLogId
# )

# Extract metadata values for convenience
NOW = datetime.now()
# EventParms = f"{metadata['load_start_date'] or '<null>'}|{metadata['load_end_date'] or '<null>'}"

# Get version numbers - incremental load, check if this is needeed
# Versioning no
# VersionNumCon = get_version_number(spark, "IndexConst", "STAGE")
# VersionNumGeo = get_version_number(spark, "IndexGeo", "STAGE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
Main ETL process orchestration
Separate this process per function/table
"""
try:
    # LOAD PROCESS
    if RunLoadProcess == 1: # -> thsi will always run step by step
        print(OBJECT_NAME, "Starting constituent load...")
        load_index_constituent(metadata)
        print(OBJECT_NAME, "IndexConstituent_Load complete.")
    else:
        print(OBJECT_NAME, "Skipping constituent load.")

    # GEO CALCULATION
    if RunCalcGeo == 1 or RunGeoCorrection == 1:
        if RunCalcGeo == 1:
            print(OBJECT_NAME, "Running GeoRegion calculation...")
            calc_geo_region(spark, metadata)
        if RunGeoCorrection == 1:
            print(OBJECT_NAME, "Running GeoRegion correction...")
            calc_geo_correction(spark, metadata)
    else:
        print(OBJECT_NAME, "Skipping Geo calculations.")

    # CLASSIFICATION CALCULATION
    if RunCalcClass == 1 or RunClassCorrection == 1:
        if RunCalcClass == 1:
            print(OBJECT_NAME, "Running Classification calculation...")
            calc_classification(spark, metadata)
        if RunClassCorrection == 1:
            print(OBJECT_NAME, "Running Classification correction...")
            calc_class_correction(spark, metadata)
    else:
        print(OBJECT_NAME, "Skipping Classification calculations.")

    # APPLY PROCESS
    if RunApplyProcess == 1:
        print("Running Apply process for Geo and Classification...")
        apply_geo_classification(spark, metadata)
    else:
        print("Skipping Apply process.")

    # Finalize log
    # finalize_etl(OBJECT_NAME)

except Exception as e:
    print(f"Error occurred: {str(e)}")
    raise e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def load_index_constituent():
    """
    ETL Function: Index Constituent Loader (PySpark for Fabric)
    """

    # Load Raw Data (drop duplicates early)
    raw_df = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/Factset")
    print(f'[DEBUG] Raw Data Count: {raw_df.count()}')

    index_df = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/vwsourcereferenceindex").filter(col("IndexStatus") != "Deleted")
    print(f'[DEBUG] Index Data Count (filtered): {index_df.count()}')

    ref_df = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/vwsourcereferenceflatten_crimsonx")
    print(f'[DEBUG] Index Data Count (filtered): {ref_df.count()}')

    index_df.createOrReplaceTempView("vwSourceReferenceIndex")
    ref_df.createOrReplaceTempView("vwSourceReferenceFlatten_CrimsonX")

    index_factset_df = spark.sql("""
    SELECT vri.*,
           vrfc.HMCObjectIdCrimsonX
    FROM vwSourceReferenceIndex vri
    JOIN vwSourceReferenceFlatten_CrimsonX vrfc
        ON vri.IndexId = vrfc.HMCObjectMasterId
    WHERE vri.IndexIdFactset IS NOT NULL
    """)


    # Join with LEFT join and index filtering
    joined_df = raw_df.join(index_factset_df, raw_df["BENCHMARK_ID"] == index_factset_df["IndexIdFactset"], how="left")
    print(f'[DEBUG] Joined Data Count: {joined_df.count()}')
    print('[DEBUG] Sample joined data:')
    joined_df.show(5, truncate=False)

    # Compose main stage_df
    stage_df = joined_df.select(
        col("BENCHMARK_ID").alias("IndexIdentifier"),
        col("SECURITY_ID").alias("SecurityIdentifier"),
        col("CONST_WEIGHT").alias("Weight"),
        when(
            (col("COUNTRY_INCORP_MSCI").isNotNull()) & (lower(col("COUNTRY_INCORP_MSCI")) != "null"),
            col("COUNTRY_INCORP_MSCI")
        ).otherwise(col("COUNTRY_RISK")).alias("Country"),

        col("DATE").alias("DataDate"),
        # col("SourceFileName"),
        # col("SourceFilePath"),
        when(col("IndexIdFactset").isNotNull(), lit("Index")).alias("LoadComment"),
        lit("Y").alias("IsValid"),
        lit("N").alias("IsNew"),
        lit("N").alias("IsDup"),
        lit("N").alias("Ignore"),
        col("IndexId"),
        lit("").alias("SectorCode"),
        col("FG_GICS_SECTOR").alias("SectorName"),
        lit("").alias("IndustryCode"),
        col("FG_GICS_INDUSTRY").alias("IndustryName"),
        lit("").alias("IndustryGrpCode"),
        col("FG_GICS_INDGRP").alias("IndustryGrpName"),
        lit("").alias("SubIndustryCode"),
        col("FG_GICS_SUBIND").alias("SubIndustryName")
    )
    print(f'[DEBUG] Stage Data Count (after select): {stage_df.count()}')
    print('[DEBUG] Sample stage data:')
    stage_df.show(5, truncate=False)

    # Apply country matching
    stage_df = apply_country_matching(stage_df, spark, bronze_lh_id)
    print(f'[DEBUG] Stage Data Count (after country matching): {stage_df.count()}')
    print('[DEBUG] Sample stage data after country matching:')
    stage_df.select("Country", "CountryId").show(5, truncate=False)

    # Validation rules
    validation_rules = {
        "CountryId": {
            "condition": col("CountryId").isNull(),
            "error_message": "ERROR: Country code not found."
        },
        "IndexId": {
            "condition": col("IndexId").isNull(),
            "error_message": "ERROR: Index not found."
        }
    }

    stage_df = validate_dataframe(stage_df, validation_rules)
    print(f'[DEBUG] Stage Data Count (after validation): {stage_df.count()}')
    print('[DEBUG] Sample stage data after validation:')
    stage_df.select("CountryId", "IndexId", "IsValid", "LoadComment").show(5, truncate=False)

    stage_df.createOrReplaceTempView("stage")

    # Country threshold revalidation
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW tempCountryCodeCount AS
    SELECT IndexId, SUM(CASE WHEN CountryId IS NULL THEN 1 ELSE 0 END) AS nullCountries
    FROM stage
    GROUP BY IndexId
    """)
    print('[DEBUG] tempCountryCodeCount:')
    spark.table("tempCountryCodeCount").show()

    stage_df = stage_df.alias("s") \
        .join(spark.table("tempCountryCodeCount").alias("t"), "IndexId") \
        .withColumn("IsValid", when(
            (col("CountryId").isNull()) & (col("nullCountries") <= MaxInvalidCountry),
            lit("Y")
        ).otherwise(col("IsValid")))
    print('[DEBUG] Final stage_df after country threshold revalidation:')
    stage_df.select('CountryId','IsValid','Country','IndexId','nullCountries').show(10, truncate=False)

    print(f'[DEBUG] Final stage_df count: {stage_df.count()}')

    # Write to tables (commented out for now)
    write_to_tables(
        stage_df,
        ic_stage_path,
        "Silver.IndexConstituent",
        "Silver.IndexConstituent_ERROR",
        truncate_stage=False
    )

    print(f"ETL Completed at: {datetime.now()}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

load_index_constituent()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calc_geo_region():
    """
    ETL Function: Calculate Geographic Region
    
    This function calculates geographic region classifications for index constituents.
    """
    # Initialize function-specific metadata
    LOADNAME = 'IndexGeo'
    
    # Define stage paths for parquet files
    constituent_stage_path = f"{bronze_lh_id}/Files/IndexConstituent_STAGE.parquet"
    geo_stage_path = f"{bronze_lh_id}/Files/IndexGeography_STAGE.parquet"
    countryreg_path =f"{bronze_lh_id}/Tables/Bronze/CrimsonXCountryRegion"

    countryreg_df = spark.read.parquet(countryreg_path)
    countryreg_df.createOrReplaceTempView("countryreg")

    # Load Stage DF
    stage_df = spark.read.parquet(ic_stage_path)
    stage_df.createOrReplaceTempView("stage")
    
    # Get invalid indices to exclude
    invalid_index_df = spark.sql("""
        SELECT DISTINCT(COALESCE(IndexId, '00000000-0000-0000-0000-000000000000')) AS IndexId
        FROM stage
        WHERE IsValid = 'N'
    """)

    invalid_index_df.createOrReplaceTempView("excludeIndex")
    
    # Insert valid data into staging with optimizations for Fabric
    valid_data_df = spark.sql("""
    SELECT ic.IndexId, 
           cr.RegionGeographicStrategyId,
           /*ic.AsOfDate,*/
           SUM(ic.Weight) AS ExposurePct,
           0 AS CorrectionPct,
           /*current_timestamp() AS UpdateTimeStamp,*/
           'IndexGeo' AS LoadType,
           /*'' AS LoadComment,*/
           'etl.HMCStaging_HMCStaging_IndexConstituent_CalcGeoRegion' AS LoadProcess,
           current_timestamp() AS LoadTimestamp,
           'Y' AS IsValid,
           'N' AS IsNew,
           'N' AS IsDup,
           'N' AS Ignore
    FROM stage ic
    JOIN countryreg cr ON ic.CountryId = cr.CountryID
    WHERE ic.IndexId NOT IN (SELECT IndexId FROM excludeIndex)
    GROUP BY ic.IndexId, /*ic.AsOfDate,*/ cr.RegionGeographicStrategyId
""")
    
    # Optimize DataFrame for Fabric
    # valid_data_df = optimize_dataframe(valid_data_df, ["IndexId", "AsOfDate"])
    valid_data_df = optimize_dataframe(valid_data_df, ["IndexId"])
    
    # Add metadata columns
    # valid_data_df = add_metadata_columns(valid_data_df, metadata)
    
    # Write to staging parquet
    valid_data_df.write.mode("overwrite").parquet(geo_stage_path)
    print(f"Inserted {valid_data_df.count()} rows to IndexGeography_STAGE")
    
    # Apply overrides
    overrides_df = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/indexgeography_override") # what table?
    stage_df = spark.read.parquet(geo_stage_path)
    
    # Optimize DataFrames for Fabric
    overrides_df = optimize_dataframe(overrides_df, ["IndexIdWarehouse", "GeographicStrategyId"])
    stage_df = optimize_dataframe(stage_df, ["IndexId", "RegionGeographicStrategyId"])
    
    updated_stage_df = stage_df.alias("stage") \
        .join(overrides_df.alias("igo"), 
              (col("stage.IndexId") == col("igo.IndexIdWarehouse")) &
              (col("stage.RegionGeographicStrategyId") == col("igo.GeographicStrategyId")), "left") \
        .withColumn("ExposurePct_Override", 
                    when(col("stage.ExposurePct").isNull(), col("ExposurePct_Override"))
                    .otherwise(col("stage.ExposurePct"))) \
        .withColumn("CorrectionPct_Override", lit(0.00)) \
        .withColumn("LoadComment", 
                    when(col("ExposurePct_Override").isNotNull(),
                         concat_ws('. ', lit("Exposure OVERRIDE"), col("igo.LoadComment")))
                    # .otherwise(col("stage.LoadComment"))
                    )
    
    # Write updated data back to staging parquet
    updated_stage_df.write.mode("overwrite").parquet(geo_stage_path)
    print("Overrides applied")
    
    # Move invalid rows to ERROR table
    invalid_rows = spark.sql("""
        SELECT ic.IndexId,
               'ERROR: Invalid constituent data./' + COALESCE(ic.LoadComment, '') AS LoadComment,
               current_timestamp() AS LoadTimestamp
        FROM stage ic
        JOIN excludeIndex ei ON ic.IndexId = ei.IndexId
    """)
    
    invalid_rows.write.mode("append").saveAsTable("Silver.IndexGeography_ERROR")
    
    # Move good rows to PROCESSED table
    good_rows = spark.read.parquet(geo_stage_path) \
        .filter((F.col("IsValid") == "Y") & (F.col("Ignore") == "N"))
    
    good_rows.write.mode("append").saveAsTable("Silver.IndexGeography")
    print("Processed rows moved to PROCESSED table")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

calc_geo_region()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calc_geo_correction():
    """
    ETL Function: Calculate Geographic Correction
    
    This function calculates correction factors for geographic region classifications.
    """

    geo_stage_path = "f{bronze_lh_id}/Files/IndexGeography_STAGE.parquet"
    
    # Index correction calculation with optimizations for Fabric
    stage = spark.read.parquet("Bronze.geo_stage_path") 
        # .filter((F.col("AsOfDate") >= F.lit(metadata['load_start_date'])))
    
    # Optimize DataFrame for Fabric
    stage = optimize_dataframe(stage, ["IndexId"])
    
    # Inner aggregation
    agg_stage = stage.groupBy("indexid", "GeographicStrategyid") \
                     .agg(F.sum("exposurepct").alias("exposurepct"))
    
    # Outer aggregation for correction
    correction_df = agg_stage.groupBy("indexid") \
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
            # (F.col("AsOfDate") >= F.lit(metadata['load_start_date'])) &
            # (F.col("AsOfDate") <= F.lit(metadata['load_end_date'])) &
            (F.col("LoadComment").contains("PLUG. missing region"))
        )
    
    # Re-apply CorrectionPct
    windowSpec = Window.partitionBy("IndexId").orderBy(F.desc("ExposurePct"))
    
    ranked = stage.withColumn("exposurerank", F.row_number().over(windowSpec)) \
                  .filter(F.col("exposurerank") == 1)
    
    stage_corrected = stage.join(
        correction_df,
        ["indexid"]
    ).join(
        ranked.select("indexid", "GeographicStrategyId"),
        ["indexid", "GeographicStrategyId"]
    ).withColumn("CorrectionPct", F.col("CorrectionPct"))
    
    # Fill in missing regions
    geo_strategies = spark.table("GeographicStrategy")
    # unique_pairs = spark.table("dbo.IndexGeography_STAGE") \
    #                     # .filter((F.col("AsOfDate") >= F.lit(metadata['load_start_date'])) &
    #                     #         (F.col("AsOfDate") <= F.lit(metadata['load_end_date']))) \
    #                     .select("indexid", "AsOfDate").distinct()
    unique_pairs = spark.table("dbo.IndexGeography_STAGE") \
                    .select("indexid", "AsOfDate") \
                    .distinct()
    
    # Optimize DataFrames for Fabric
    geo_strategies = optimize_dataframe(geo_strategies)
    unique_pairs = optimize_dataframe(unique_pairs, ["indexid"])
    
    missing_region = geo_strategies.crossJoin(unique_pairs)
    
    # existing = spark.table("dbo.IndexGeography_STAGE") \
    #                 .filter((F.col("AsOfDate") >= F.lit(metadata['load_start_date'])) &
    #                         (F.col("AsOfDate") <= F.lit(metadata['load_end_date']))) \
    #                 .select("indexid", "GeographicStrategyId", "AsOfDate")
    existing = spark.table("dbo.IndexGeography_STAGE") \
                .select("indexid", "GeographicStrategyId")

    
    # Optimize DataFrame for Fabric
    existing = optimize_dataframe(existing, ["indexid", "GeographicStrategyId"])
    
    missing_to_insert = missing_region.join(
        existing,
        ["indexid", "GeographicStrategyId"],
        "leftanti"
    )
    
    # Add metadata columns
    missing_to_insert = add_metadata_columns(
        missing_to_insert.withColumn("ExposurePct", F.lit(0.0))
                         .withColumn("CorrectionPct", F.lit(0.0)),
        metadata,
        load_comment="PLUG. missing region"
    )
    
    # Write to stage table
    # missing_to_insert.write.mode("append").saveAsTable("dbo.IndexGeography_STAGE")
    missing_to_insert.write.mode("append").parquet(geo_stage_path)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

calc_geo_correction()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calc_classification():
    """
    ETL Function: Calculate Classification
    
    This function calculates classification data for index constituents.
    """
    # Initialize function-specific metadata
    OBJECT_NAME = "etl.HMCStaging_HMCStaging_IndexConstituent_CalcClassification"
    
    # Get invalid indices to exclude
    exclude_index_df = get_invalid_indices(spark)
    
    # Truncate staging table
    spark.sql("TRUNCATE TABLE dbo.IndexClassification_STAGE")
    
    # Insert valid data into staging with optimizations for Fabric
    valid_classification_df = spark.sql(f"""
        SELECT 
            ic.IndexId, ic.AsOfDate, 
            ic.SectorCode, ic.SectorName,
            ic.IndustryCode, ic.IndustryName,
            ic.IndustryGrpCode, ic.IndustryGrpName,
            ic.SubIndustryCode, ic.SubIndustryName,
            SUM(COALESCE(ic.Weight, 0)) AS ClassificationPct,
            0 AS CorrectionPct
        FROM dbo.IndexConstituent_STAGE ic
        LEFT ANTI JOIN excludeIndex x ON COALESCE(ic.IndexId, '00000000-0000-0000-0000-000000000000') = x.IndexId
        GROUP BY 
            ic.IndexId, ic.AsOfDate, ic.SectorCode, ic.SectorName,
            ic.IndustryCode, ic.IndustryName,
            ic.IndustryGrpCode, ic.IndustryGrpName,
            ic.SubIndustryCode, ic.SubIndustryName
    """)
    
    # Optimize DataFrame for Fabric
    valid_classification_df = optimize_dataframe(valid_classification_df, ["IndexId", "AsOfDate"])
    
    # Add metadata columns
    valid_classification_df = add_metadata_columns(valid_classification_df, metadata)
    
    # Write to staging table
    valid_classification_df.write.mode("append").saveAsTable("dbo.IndexClassification_STAGE")
    
    # Data Quality Check - Null SectorName
    stage_df = spark.table("dbo.IndexClassification_STAGE")
    stage_df = stage_df.withColumn(
        "IsValid", 
        when(col("SectorName").isNull(), lit("N")).otherwise(col("IsValid"))
    ).withColumn(
        "LoadComment",
        when(col("SectorName").isNull(), 
             concat_ws(". ", lit("ERROR: Sector NULL"), col("LoadComment")))
        .otherwise(col("LoadComment"))
    )
    
    # Write updated data back to staging
    stage_df.write.mode("overwrite").saveAsTable("dbo.IndexClassification_STAGE")
    
    # Move invalid rows to ERROR table
    invalid_rows = spark.sql("""
        SELECT 
            ic.IndexId, ic.IndexIdentifier, ic.SecurityIdentifier,
            ic.AsOfDate, ic.SectorCode, ic.SectorName,
            ic.IndustryCode, ic.IndustryName,
            ic.IndustryGrpCode, ic.IndustryGrpName,
            ic.SubIndustryCode, ic.SubIndustryName,
            NULL AS ClassificationPct, NULL AS CorrectionPct,
            CURRENT_TIMESTAMP(), '{metadata['user_id']}' AS UpdateByHMCUserId,
            ic.LoadType,
            CONCAT('ERROR: Invalid constituent data./', 
                   CASE WHEN ic.LoadComment IS NOT NULL THEN '. ' || ic.LoadComment ELSE '' END),
            ic.LoadComment,
            ic.LoadProcess, CURRENT_TIMESTAMP(), ic.LoadStartDate, ic.EventLogId, ic.VersionNum,
            ic.IsValid, ic.IsNew, ic.IsDup, ic.Ignore
        FROM dbo.IndexConstituent_STAGE ic
        JOIN excludeIndex x ON COALESCE(ic.IndexId, '00000000-0000-0000-0000-000000000000') = x.IndexId
    """)
    
    invalid_rows.write.mode("append").saveAsTable("dbo.IndexClassification_ERROR")
    
    # Move valid rows to PROCESSED table
    good_rows = spark.sql("""
        SELECT *
        FROM dbo.IndexClassification_STAGE
        WHERE IsValid = 'Y' AND Ignore = 'N'
    """)
    
    good_rows.write.mode("append").saveAsTable("dbo.IndexClassification_PROCESSED")
    
    # Log completion
    log_message(OBJECT_NAME, "Load Complete.")
    
    # Cleanup
    spark.catalog.dropTempView("excludeIndex")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def calc_class_correction(spark, metadata):
    """
    ETL Function: Calculate Classification Correction
    
    This function calculates correction factors for classification data.
    """
    # Initialize function-specific metadata
    OBJECT_NAME = 'etl.HMCStaging_HMCStaging_IndexConstituent_CalcClassCorrection'
    
    # Create a temporary view of correction percentages
    stage_df = spark.table("dbo.IndexClassification_STAGE")
    
    # Optimize DataFrame for Fabric
    stage_df = optimize_dataframe(stage_df, ["IndexId", "AsOfDate"])
    
    # Calculate correction percentages
    agg_df = stage_df.groupBy("IndexId", "AsOfDate") \
                     .agg(F.sum("ClassificationPct").alias("TotalPct"))
    
    correction_df = agg_df.withColumn("CorrectionPct", F.lit(100) - F.col("TotalPct"))
    correction_df.createOrReplaceTempView("ClassificationCorrection")
    
    # Reset prior correction values
    spark.sql("""
        UPDATE dbo.IndexClassification_STAGE
        SET CorrectionPct = 0
        WHERE AsOfDate >= '{metadata['load_start_date']}'
          AND AsOfDate <= '{metadata['load_end_date']}'
    """)
    
    # Apply correction to highest classification percentage
    windowSpec = Window.partitionBy("IndexId", "AsOfDate").orderBy(F.desc("ClassificationPct"))
    
    # Get top classification per index/date
    top_class_df = stage_df.withColumn("rank", F.row_number().over(windowSpec)) \
                          .filter(F.col("rank") == 1) \
                          .drop("rank")
    
    # Join with correction values
    corrected_df = top_class_df.join(
        correction_df,
        ["IndexId", "AsOfDate"]
    ).withColumn("CorrectionPct", F.col("CorrectionPct"))
    
    # Update staging table with corrections
    for row in corrected_df.collect():
        spark.sql(f"""
            UPDATE dbo.IndexClassification_STAGE
            SET CorrectionPct = {row['CorrectionPct']}
            WHERE IndexId = '{row['IndexId']}'
              AND AsOfDate = '{row['AsOfDate']}'
              AND SectorCode = '{row['SectorCode']}'
              AND IndustryCode = '{row['IndustryCode']}'
        """)
    
    # Log completion
    finalize_etl(OBJECT_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def apply_geo_classification(spark, metadata):
    """
    ETL Function: Apply Geography and Classification
    
    This function applies the final geographic and classification data to the warehouse.
    """
    # Initialize function-specific metadata
    OBJECT_NAME = 'etl.HMCStaging_DataWarehouse_IndexGeography_Apply'
    
    # Apply Geography
    log_message(OBJECT_NAME, "Applying Geography data to warehouse...")
    
    # Get processed geography data
    geo_df = spark.table("dbo.IndexGeography_PROCESSED") \
                  .filter((F.col("AsOfDate") >= F.lit(metadata['load_start_date'])) &
                          (F.col("AsOfDate") <= F.lit(metadata['load_end_date'])))
    
    # Optimize DataFrame for Fabric
    geo_df = optimize_dataframe(geo_df, ["IndexId", "AsOfDate"])
    
    # Write to warehouse table
    geo_df.write.mode("append").saveAsTable("HMCDataWarehouse.dbo.IndexGeography")
    
    # Apply Classification
    OBJECT_NAME = 'etl.HMCStaging_DataWarehouse_IndexClassification_Apply'
    log_message(OBJECT_NAME, "Applying Classification data to warehouse...")
    
    # Get processed classification data
    class_df = spark.table("dbo.IndexClassification_PROCESSED") \
                    .filter((F.col("AsOfDate") >= F.lit(metadata['load_start_date'])) &
                            (F.col("AsOfDate") <= F.lit(metadata['load_end_date'])))
    
    # Optimize DataFrame for Fabric
    class_df = optimize_dataframe(class_df, ["IndexId", "AsOfDate"])
    
    # Write to warehouse table
    class_df.write.mode("append").saveAsTable("HMCDataWarehouse.dbo.IndexClassification")
    
    # Log completion
    log_message(OBJECT_NAME, "Apply process complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
