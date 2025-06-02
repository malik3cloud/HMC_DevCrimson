# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

"""
Utility functions for ETL operations in Microsoft Fabric
This module contains reusable functions for common ETL operations
"""

from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import Optional, List, Dict, Any, Union


def get_spark_session() -> SparkSession:
    """Get or create a Spark session"""
    return SparkSession.builder.getOrCreate()


def log_message(object_name: str, message: str) -> None:
    """Log a message with timestamp and object name"""
    print(f"{object_name}: {message}")


# Skip this
def get_system_user_id(spark: SparkSession) -> str:
    """Get the system account user ID"""
    return spark.sql("""
        SELECT HMCUserId 
        FROM HMCDataWarehouse.dbo.HMCUser 
        WHERE DisplayName = 'SYSTEMACCOUNT'
    """).collect()[0][0]


def get_version_number(spark: SparkSession, load_name: str, version_type: str = 'STAGE') -> int:
    """Get the version number for a specific load name and type"""
    result = spark.sql(f"""
        SELECT VersionNum
        FROM HMCDataWarehouse.util.Version
        WHERE versionName = '{load_name}' AND versionType = '{version_type}'
    """).collect()
    
    if result and len(result) > 0:
        return result[0][0]
    return 101  # Default version number


def get_config_value(spark: SparkSession, config_name: str, default_value: Any = None) -> Any:
    """Get a configuration value from the config table"""
    now = datetime.now()
    result = spark.sql(f"""
        SELECT configValue
        FROM util.config
        WHERE configName = '{config_name}'
        AND startDate <= current_date()
        AND endDate >= current_date()
    """).collect()
    
    if result and len(result) > 0:
        return result[0][0]
    return default_value


def get_load_dates(spark: SparkSession, load_name: str) -> tuple:
    """Get load start and end dates from config or use defaults"""
    load_start_date = get_config_value(
        spark, 
        f"{load_name}_LoadStartDate", 
        date(2020, 1, 1)
    )
    load_end_date = date.today()
    
    return load_start_date, load_end_date

# util config table reference
def get_max_invalid_country(spark: SparkSession, load_name: str) -> int:
    """Get maximum allowed invalid countries from config"""
    return int(get_config_value(spark, f"{load_name}_MaxInvalidCtry", 10))


def initialize_metadata(
    spark: SparkSession, 
    object_name: str,
    load_name: str,
    # version_num: Optional[int] = None,
    # load_start_date: Optional[date] = None,
    # load_end_date: Optional[date] = None,
    # event_log_id: Optional[int] = None
) -> Dict[str, Any]:
    """Initialize metadata for ETL operations"""
    now = datetime.now()
    
    # Get user ID
    # user_id = get_system_user_id(spark)
    
    # Get version number if not provided
    # if version_num is None:
    #     version_num = get_version_number(spark, load_name)
    
    # Get load dates if not provided
    # if load_start_date is None or load_end_date is None:
    #     load_start_date, load_end_date = get_load_dates(spark, load_name)
    
    # Log start
    # log_message(object_name, f"Begin at {now}")
    # log_message(object_name, f"Parameters: LoadStartDate={load_start_date}, LoadEndDate={load_end_date}, VersionNum={version_num}")
    
    return {
        "object_name": object_name,
        "load_name": load_name,
        "now": now,
        # "user_id": user_id,
        # "version_num": version_num,
        # "load_start_date": load_start_date,
        # "load_end_date": load_end_date,
        # "event_log_id": event_log_id
    }


def add_metadata_columns(
    df: DataFrame, 
    metadata: Dict[str, Any],
    is_valid: str = "Y",
    load_comment: Optional[str] = None
) -> DataFrame:
    """Add standard metadata columns to a DataFrame"""
    return df.withColumn("LoadType", F.lit(metadata["load_name"])) \
        .withColumn("LoadComment", F.when(load_comment is not None, F.lit(load_comment)).otherwise(F.lit(None))) \
        .withColumn("LoadProcess", F.lit(metadata["object_name"])) \
        .withColumn("LoadTimestamp", F.lit(metadata["now"])) \
        .withColumn("LoadStartDate", F.lit(metadata["load_start_date"])) \
        .withColumn("EventLogId", F.lit(metadata["event_log_id"])) \
        .withColumn("VersionNum", F.lit(metadata["version_num"])) \
        .withColumn("IsValid", F.lit(is_valid)) \
        .withColumn("IsNew", F.lit("N")) \
        .withColumn("IsDup", F.lit("N")) \
        .withColumn("Ignore", F.lit("N")) \
        .withColumn("UpdateTimeStamp", F.lit(metadata["now"])) \
        .withColumn("UpdateByHMCUserId", F.lit(metadata["user_id"]))


def get_invalid_indices(spark: SparkSession, stage_table:str) -> DataFrame:
    """Get invalid index IDs from the constituent stage table"""
    invalid_index_df = spark.sql(f"""
        SELECT DISTINCT(COALESCE(IndexId, '00000000-0000-0000-0000-000000000000')) AS IndexId
        FROM {stage_table}
        WHERE IsValid = 'N'
    """)
    invalid_index_df.createOrReplaceTempView("excludeIndex")
    return invalid_index_df


def write_to_tables(
    df: DataFrame, 
    stage_path: str, 
    processed_table: str, 
    error_table: str,
    truncate_stage: bool = True
) -> None:
    """Write data to stage, processed, and error tables"""
    spark = get_spark_session()
    
    # Truncate stage table if requested
    if truncate_stage:
        spark.sql(f"TRUNCATE TABLE {stage_table}")
    
    # Write to stage parquet
    df.write.mode("append").format("parquet").save(stage_path)
    
    # Write valid records to processed table
    valid_records = df.filter((F.col("IsValid") == "Y") & (F.col("Ignore") == "N"))
    valid_records.write.mode("append").format("delta").saveAsTable(processed_table)
    
    # Write invalid records to error table
    invalid_records = df.filter((F.col("IsValid") == "N") | (F.col("Ignore") == "Y"))
    invalid_records.write.mode("append").format("delta").saveAsTable(error_table)


def finalize_etl(object_name: str) -> None:
    """Log completion of ETL process"""
    log_message(object_name, f"End at {datetime.now()}")


def optimize_dataframe(df: DataFrame, partition_cols: List[str] = None) -> DataFrame:
    """Apply Fabric-specific optimizations to a DataFrame"""
    # Cache frequently used DataFrames
    df = df.cache()
    
    # Apply repartitioning if partition columns are provided
    if partition_cols and len(partition_cols) > 0:
        df = df.repartition(*partition_cols)
    
    return df


def apply_country_matching(df: DataFrame, spark: SparkSession, bronze_lh_id:str) -> DataFrame:
    """Apply country matching logic to a DataFrame"""
    country_df = spark.read.format("delta").load(f"{bronze_lh_id}/Tables/Bronze/CrimsonXCountry")
    
    return df.join(country_df, df["Country"] == country_df["ISOCode2"], "left") \
        .withColumn("CountryId", F.col("CountryID")) \
        .drop("CountryID") \
        .withColumn("LoadComment", F.concat_ws(". ", F.col("LoadComment"), F.lit("Currency")))


def validate_dataframe(
    df: DataFrame, 
    validation_rules: Dict[str, Dict[str, Any]]
) -> DataFrame:
    """Apply validation rules to a DataFrame"""
    result_df = df

    for column, rule in validation_rules.items():
        condition = rule.get("condition")
        error_message = rule.get("error_message", f"ERROR: {column} validation failed")

        if condition is not None:
            result_df = result_df.withColumn(
                "IsValid", 
                F.when(condition & (F.col("IsValid") == "Y"), "N").otherwise(F.col("IsValid"))
            ).withColumn(
                "LoadComment",
                F.when(condition & (F.col("IsValid") == "N"), F.lit(error_message))
                .otherwise(F.col("LoadComment"))
            )
    
    return result_df


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def test_utils() -> str:
    return 'Succesfully Ran Factset Utils'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
