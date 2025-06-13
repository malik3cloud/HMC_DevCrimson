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
from pyspark.sql.functions import broadcast


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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# Build this from standard nbs
bronze_lh_id = 'abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/13ef97da-5da2-466d-8c5f-2a70572c6558'
silver_lh_id = 'abfss://33535eb8-4d07-49bc-b3a5-cc91d3aa6ced@onelake.dfs.fabric.microsoft.com/e9fc4e80-ff69-4d45-bbdd-892592889465'
ic_stage_path = f"{bronze_lh_id}/Files/IndexConstituent_STAGE.parquet"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# From the new requirement

# MARKDOWN ********************

# ## IndexRegionCountry

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import current_timestamp

def generate_index_country_exposure(
    bronze_lh_id: str,
    silver_lh_id: str,
    indexid_filter: str = None
) -> None:
    output_path = f"{silver_lh_id}/Tables/Silver/IndexRegionCountryExposure"
    error_path = f"{silver_lh_id}/Tables/Silver/IndexRegionCountryExposure_Error"

    #load source tables
    src_ref_index = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/HMCDataWarehousevwSourceReferenceIndex")
    src_ref_flatten = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/HMCDataWarehousevwSourceReferenceFlatten_CrimsonX")\
        .filter(F.col("HMCObjectSourceSystem") == "FACTSET")
    factset = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/Factset")
    country_region = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/CrimsonXCountryRegion")
    geographic_strategy = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/CrimsonXGeographicStrategy")
    country = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/CrimsonXCountry")

    #build index reference
    index_ref_df = (
        src_ref_index.alias("vri")
        .join(
            src_ref_flatten.alias("vrfc"),
            F.lower(F.trim(F.col("vri.IndexId"))) == F.lower(F.trim(F.col("vrfc.HMCObjectMasterId")))
        )
        .join(
            factset.alias("fc"),
            F.col("fc.BENCHMARK_ID") == F.col("vri.IndexIdFactset"),
            how="left"
        )
        .selectExpr(
            "vrfc.HMCObjectIdCrimsonX as IndexId",
            "vri.IndexIdFactset",
            "fc.*"
        )
    )
    print(f"index_ref_df count: {index_ref_df.count()}")

    # FactSet with IndexId and CountryID_numeric
    factset_with_index = (
        factset.alias("fc")
        .join(
            src_ref_index.alias("vri"),
            F.col("fc.BENCHMARK_ID") == F.col("vri.IndexIdFactset")
        )
        .join(
            src_ref_flatten.alias("vrfc"),
            F.lower(F.trim(F.col("vri.IndexId"))) == F.lower(F.trim(F.col("vrfc.HMCObjectMasterId")))
        )
        .join(
            country.alias("c"),
            F.col("fc.COUNTRY_RISK") == F.col("c.ISOCode2"),
            how="left"
        )
        .select(
            "fc.BENCHMARK_ID",
            "fc.DATE",
            "fc.CONST_WEIGHT",
            F.col("vrfc.HMCObjectIdCrimsonX").alias("IndexId"),
            F.col("c.CountryID").alias("CountryID_numeric")
        )
    )
    print(f"factset_with_index count: {factset_with_index.count()}")

    #compute total CONST_WEIGHT per IndexId, Country, Region, and Date
    exposure_with_region = (
        factset_with_index
        .join(country_region.alias("cr"), F.col("CountryID_numeric") == F.col("cr.CountryID"))
        .join(geographic_strategy.alias("gs"), F.col("cr.RegionGeographicStrategyId") == F.col("gs.GeographicStrategyId"))
        .join(country.alias("c"), F.col("CountryID_numeric") == F.col("c.CountryID"))
        .select(
            "IndexId",
            "DATE",
            F.col("gs.Description").alias("GeographicRegion"),
            F.col("c.CountryName").alias("Country"),
            "CONST_WEIGHT"
        )
    )

    # aggregate Exposure per group
    exposure_agg_by_date = (
        exposure_with_region
        .groupBy("IndexId", "GeographicRegion", "Country", "DATE")
        .agg(F.sum("CONST_WEIGHT").alias("Exposure"))
    )
    exposure_agg_by_date.show()

    #identify latest date per IndexId, Region, Country
    latest_date_per_group = (
        exposure_agg_by_date
        .groupBy("IndexId", "GeographicRegion", "Country")
        .agg(F.max("DATE").alias("LatestDate"))
    )

    #join to filter only the latest records
    final_df = (
        exposure_agg_by_date.alias("e")
        .join(
            latest_date_per_group.alias("l"),
            (F.col("e.IndexId") == F.col("l.IndexId")) &
            (F.col("e.GeographicRegion") == F.col("l.GeographicRegion")) &
            (F.col("e.Country") == F.col("l.Country")) &
            (F.col("e.DATE") == F.col("l.LatestDate")),
            how="inner"
        )
        .select("e.IndexId", "e.GeographicRegion", "e.Country", "e.Exposure")
    )

    #optional filter by specific IndexId
    if indexid_filter:
        final_df = final_df.filter(F.col("IndexId") == indexid_filter)

    final_df.show(10)
    print(f"final_df count: {final_df.count()}")
    final_df.write.format("delta").mode("overwrite").save(output_path)
    print(f"Exposure written to: {output_path}")

    #handle missing FactSet benchmark mappings
    error_df = (
        src_ref_index.alias("vri")
        .join(
            src_ref_flatten.alias("vrfc"),
            F.lower(F.trim(F.col("vri.IndexId"))) == F.lower(F.trim(F.col("vrfc.HMCObjectMasterId")))
        )
        .filter(F.col("vri.IndexIdFactset").isNotNull())
        .join(
            factset,
            F.col("vri.IndexIdFactset") == factset["BENCHMARK_ID"],
            how="left_anti"
        )
        .selectExpr(
            "vrfc.HMCObjectIdCrimsonX as IndexId",
            "vri.IndexIdFactset as FactsetId",
            "current_timestamp() as DateTime",
            "'Factset BENCHMARK_ID not found for IndexIdFactset' as Error"
        )
    )

    print(f"error_df count: {error_df.count()}")
    error_df.write.format("delta").mode("overwrite").save(error_path)
    print(f"Error written to: {error_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generate_index_country_exposure(bronze_lh_id, silver_lh_id)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## IndexRegion

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import current_timestamp

def generate_index_region_exposure(
    bronze_lh_id: str,
    silver_lh_id: str,
    indexid_filter: str = None
) -> None:
    output_path = f"{silver_lh_id}/Tables/Silver/IndexRegionExposure"
    error_path = f"{silver_lh_id}/Tables/Silver/IndexRegionExposure_Error"

    # Load source tables
    src_ref_index = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/HMCDataWarehousevwSourceReferenceIndex")
    src_ref_flatten = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/HMCDataWarehousevwSourceReferenceFlatten_CrimsonX")\
        .filter(F.col("HMCObjectSourceSystem") == "FACTSET")
    factset = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/Factset")
    country_region = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/CrimsonXCountryRegion")
    geographic_strategy = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/CrimsonXGeographicStrategy")
    country = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/CrimsonXCountry")

    # Join FactSet with index and country
    factset_with_index = (
        factset.alias("fc")
        .join(
            src_ref_index.alias("vri"),
            F.col("fc.BENCHMARK_ID") == F.col("vri.IndexIdFactset")
        )
        .join(
            src_ref_flatten.alias("vrfc"),
            F.lower(F.trim(F.col("vri.IndexId"))) == F.lower(F.trim(F.col("vrfc.HMCObjectMasterId")))
        )
        .join(
            country.alias("c"),
            F.col("fc.COUNTRY_RISK") == F.col("c.ISOCode2"),
            how="left"
        )
        .select(
            "fc.BENCHMARK_ID",
            "fc.DATE",
            "fc.CONST_WEIGHT",
            F.col("vrfc.HMCObjectIdCrimsonX").alias("IndexId"),
            F.col("c.CountryID").alias("CountryID_numeric")
        )
    )

    # Join for geographic region only (omit country name)
    exposure_with_region = (
        factset_with_index
        .join(country_region.alias("cr"), F.col("CountryID_numeric") == F.col("cr.CountryID"))
        .join(geographic_strategy.alias("gs"), F.col("cr.RegionGeographicStrategyId") == F.col("gs.GeographicStrategyId"))
        .select(
            "IndexId",
            "DATE",
            F.col("gs.Description").alias("GeographicRegion"),
            "CONST_WEIGHT"
        )
    )

    # Aggregate by IndexId, Region, and Date
    exposure_agg_by_date = (
        exposure_with_region
        .groupBy("IndexId", "GeographicRegion", "DATE")
        .agg(F.sum("CONST_WEIGHT").alias("Exposure"))
    )

    # Get latest date per IndexId and Region
    latest_date_per_group = (
        exposure_agg_by_date
        .groupBy("IndexId", "GeographicRegion")
        .agg(F.max("DATE").alias("LatestDate"))
    )

    # Build distinct set of all IndexId-Region pairs
    all_index_region_pairs = (
        factset_with_index
        .join(country_region.alias("cr"), F.col("CountryID_numeric") == F.col("cr.CountryID"))
        .join(geographic_strategy.alias("gs"), F.col("cr.RegionGeographicStrategyId") == F.col("gs.GeographicStrategyId"))
        .select("IndexId", F.col("gs.Description").alias("GeographicRegion"))
        .distinct()
    )

    # Join aggregated exposure to the full set
    exposure_with_all_combos = (
        all_index_region_pairs.alias("all")
        .join(
            exposure_agg_by_date.alias("agg"),
            (F.col("all.IndexId") == F.col("agg.IndexId")) &
            (F.col("all.GeographicRegion") == F.col("agg.GeographicRegion")),
            how="left"
        )
        .select(
            F.col("all.IndexId"),
            F.col("all.GeographicRegion"),
            F.col("agg.DATE"),
            F.coalesce(F.col("agg.Exposure"), F.lit(0.0)).alias("Exposure")
        )
    )

    # Now get the latest date per IndexId + Region from all available dates
    latest_date_per_group = (
        exposure_with_all_combos
        .groupBy("IndexId", "GeographicRegion")
        .agg(F.max("DATE").alias("LatestDate"))
    )

    # Filter to only latest record per group
    final_df = (
        exposure_with_all_combos.alias("e")
        .join(
            latest_date_per_group.alias("l"),
            (F.col("e.IndexId") == F.col("l.IndexId")) &
            (F.col("e.GeographicRegion") == F.col("l.GeographicRegion")) &
            (F.col("e.DATE") == F.col("l.LatestDate")),
            how="inner"
        )
        .select("e.IndexId", "e.GeographicRegion", "e.Exposure")
    )


    # Optional: filter by IndexId
    if indexid_filter:
        final_df = final_df.filter(F.col("IndexId") == indexid_filter)

    final_df.show(10)
    print(f"final_df count: {final_df.count()}")
    final_df.write.format("delta").mode("overwrite").save(output_path)
    print(f"Exposure written to: {output_path}")

    # Error handling (same as before)
    error_df = (
        src_ref_index.alias("vri")
        .join(
            src_ref_flatten.alias("vrfc"),
            F.lower(F.trim(F.col("vri.IndexId"))) == F.lower(F.trim(F.col("vrfc.HMCObjectMasterId")))
        )
        .filter(F.col("vri.IndexIdFactset").isNotNull())
        .join(
            factset,
            F.col("vri.IndexIdFactset") == factset["BENCHMARK_ID"],
            how="left_anti"
        )
        .selectExpr(
            "vrfc.HMCObjectIdCrimsonX as IndexId",
            "vri.IndexIdFactset as FactsetId",
            "current_timestamp() as DateTime",
            "'Factset BENCHMARK_ID not found for IndexIdFactset' as Error"
        )
    )

    print(f"error_df count: {error_df.count()}")
    error_df.write.format("delta").mode("overwrite").save(error_path)
    print(f"Error written to: {error_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generate_index_region_exposure(bronze_lh_id, silver_lh_id)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## SectorIndustry

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import current_timestamp

def generate_index_sector_industry_exposure(
    bronze_lh_id: str,
    silver_lh_id: str,
    indexid_filter: str = None
) -> None:
    output_path = f"{silver_lh_id}/Tables/Silver/IndexSectorIndustryExposure"
    error_path = f"{silver_lh_id}/Tables/Silver/IndexSectorIndustryExposure_Error"

    #load source tables
    src_ref_index = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/HMCDataWarehousevwSourceReferenceIndex")
    src_ref_flatten = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/HMCDataWarehousevwSourceReferenceFlatten_CrimsonX")\
        .filter(F.col("HMCObjectSourceSystem") == "FACTSET")
    factset = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/Factset")
    sector_industry_class = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/CrimsonXSectorIndustryClassification")
    sector = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/CrimsonXSector")
    industry = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/CrimsonXSectorIndustry")

    #prepare factset
    factset = factset.select(
        "BENCHMARK_ID",
        "CONST_WEIGHT",
        "DATE",
        F.trim(F.col("FACTSET_SECTOR_CODE").cast("string")).alias("SECTOR_CODE"),
        F.trim(F.col("FACTSET_INDUSTRY_CODE").cast("string")).alias("INDUSTRY_CODE")
    )

    #join with sector/industry classifications
    factset_with_class = (
        factset.alias("fc")
        .join(sector_industry_class.alias("sic"),
              (F.col("fc.SECTOR_CODE") == F.col("sic.SectorCode")) &
              (F.col("fc.INDUSTRY_CODE") == F.col("sic.IndustryCode")),
              how="left")
        .join(sector.alias("s"), F.col("sic.SectorCode") == F.col("s.SectorCode"), how="left")
        .join(industry.alias("i"), F.col("sic.IndustryCode") == F.col("i.IndustryCode"), how="left")
        .select(
            "fc.BENCHMARK_ID",
            "fc.CONST_WEIGHT",
            "fc.DATE",
            "sic.SectorIndustryClassificationId",
            "s.SectorDescription",
            "i.IndustryDescription"
        )
    )

    #join with index reference
    factset_with_indexid = (
        factset_with_class.alias("f")
        .join(src_ref_index.alias("vri"), F.col("f.BENCHMARK_ID") == F.col("vri.IndexIdFactset"), how="inner")
        .join(src_ref_flatten.alias("vrfc"), F.col("vri.IndexId") == F.col("vrfc.HMCObjectMasterId"))
        .select(
            F.col("vrfc.HMCObjectIdCrimsonX").alias("IndexId"),
            F.col("f.SectorDescription"),
            F.col("f.IndustryDescription"),
            F.col("f.CONST_WEIGHT"),
            F.col("f.DATE")
        )
        .filter(
            F.col("SectorDescription").isNotNull() &
            F.col("IndustryDescription").isNotNull() &
            F.col("CONST_WEIGHT").isNotNull()
        )
    )

    #find latest DATE per IndexId + Sector + Industry
    latest_date_df = (
        factset_with_indexid
        .groupBy("IndexId", "SectorDescription", "IndustryDescription")
        .agg(F.max("DATE").alias("LatestDate"))
    )

    #join back to keep only rows with latest DATE
    factset_latest = (
        factset_with_indexid.alias("f")
        .join(
            latest_date_df.alias("ld"),
            (F.col("f.IndexId") == F.col("ld.IndexId")) &
            (F.col("f.SectorDescription") == F.col("ld.SectorDescription")) &
            (F.col("f.IndustryDescription") == F.col("ld.IndustryDescription")) &
            (F.col("f.DATE") == F.col("ld.LatestDate")),
            how="inner"
        )
        .drop("LatestDate")
    )

    #aggregate Exposure
    exposure_df = (
        factset_latest
        .groupBy("f.IndexId", "f.SectorDescription", "f.IndustryDescription")
        .agg(F.sum("CONST_WEIGHT").alias("Exposure"))
        .withColumnRenamed("SectorDescription", "Sector")
        .withColumnRenamed("IndustryDescription", "Industry")
    )

    if indexid_filter:
        exposure_df = exposure_df.filter(F.col("IndexId") == indexid_filter)

    exposure_df.show(10)
    print(f"Exposure output count: {exposure_df.count()}")
    exposure_df.write.format("delta").mode("overwrite").save(output_path)
    print(f"Exposure written to: {output_path}")

    #generate error table for unmatched benchmark IDs
    factset_raw = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/Factset")
    error_df = (
        src_ref_index.alias("vri")
        .join(src_ref_flatten.alias("vrfc"), F.col("vri.IndexId") == F.col("vrfc.HMCObjectMasterId"))
        .filter(F.col("vri.IndexIdFactset").isNotNull())
        .join(factset_raw, F.col("vri.IndexIdFactset") == factset_raw["BENCHMARK_ID"], how="left_anti")
        .selectExpr(
            "vrfc.HMCObjectIdCrimsonX as IndexId",
            "vri.IndexIdFactset as FactsetId",
            "current_timestamp() as DateTime",
            "'Factset BENCHMARK_ID not found for IndexIdFactset' as Error"
        )
    )

    error_df.show(10)
    print(f"Error output count: {error_df.count()}")
    error_df.write.format("delta").mode("overwrite").save(error_path)
    print(f"Error log written to: {error_path}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

generate_index_sector_industry_exposure(bronze_lh_id, silver_lh_id)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
