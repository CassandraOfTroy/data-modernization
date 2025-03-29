from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, Optional, List
import datetime

from ..utils.database import read_sql_server_table
from ..utils.logging import get_logger
from ..utils.config import APP_CONFIG

logger = get_logger(__name__)

def get_bronze_path(table_name: str) -> str:
    """Constructs the full ADLS path for a bronze table."""
    adls_config = APP_CONFIG.get('adls', {})
    base_path = f"abfss://{adls_config.get('container_name')}@{adls_config.get('storage_account_name')}.dfs.core.windows.net"
    bronze_path_prefix = adls_config.get('bronze_path', '/bronze')
    # Sanitize table name for path (e.g., dbo.Customers -> dbo_Customers)
    safe_table_name = table_name.replace('.', '_')
    return f"{base_path}{bronze_path_prefix}/{safe_table_name}"

def extract_and_write_bronze(
    spark: SparkSession,
    table_name: str,
    process_date: datetime.date,
    read_options: Optional[dict] = None,
    write_mode: str = "overwrite" # Or "append"
) -> bool:
    """Extracts data from a source SQL table and writes it to the bronze layer as a Delta table.

    Args:
        spark: The active SparkSession.
        table_name: The source table name (e.g., 'dbo.Customers').
        process_date: The date for which the data is processed (used for partitioning).
        read_options: Optional dictionary of options for the JDBC read.
        write_mode: Spark write mode ('overwrite' or 'append').

    Returns:
        True if successful, False otherwise.
    """
    logger.info(f"Starting bronze extraction for table: {table_name}")

    # 1. Read from Source
    df = read_sql_server_table(spark, table_name, options=read_options)

    if df is None:
        logger.error(f"Failed to read data for {table_name}. Aborting bronze load.")
        return False

    # 2. Add Bronze Metadata
    df_with_metadata = df.withColumn("_ingestion_timestamp", F.current_timestamp()) \
                         .withColumn("_ingestion_date", F.lit(process_date).cast("date"))
                         # Add other metadata if needed (e.g., source filename)

    # 3. Write to Bronze Delta Table (Partitioned by ingestion date)
    bronze_table_path = get_bronze_path(table_name)
    logger.info(f"Writing {table_name} data to Bronze path: {bronze_table_path}")

    try:
        (df_with_metadata.write
         .format("delta")
         .mode(write_mode)
         .partitionBy("_ingestion_date") # Partitioning improves query performance
         .option("overwriteSchema", "true") # Allows schema evolution if needed
         .save(bronze_table_path))
        logger.info(f"Successfully wrote {table_name} to {bronze_table_path} in mode '{write_mode}'")
        return True
    except Exception as e:
        logger.error(f"Error writing {table_name} to Bronze path {bronze_table_path}: {e}", exc_info=True)
        return False

def run_bronze_extraction(spark: SparkSession, process_date: datetime.date) -> Dict[str, DataFrame]:
    """Runs the bronze extraction process for all required source tables.

    Args:
        spark: The active SparkSession.
        process_date: The processing date.

    Returns:
        A dictionary mapping table names to their Bronze DataFrames (read back after writing).
           Returns None for a table if extraction or writing failed.
    """
    # Tables identified from CustomerRFM_Analysis.SQL
    source_tables = [
        "dbo.Customers",
        "dbo.CustomerAddresses",
        "dbo.CustomerInteractions",
        "dbo.Orders",
        "dbo.OrderDetails",
        "dbo.Products",
        "dbo.Returns"
    ]

    extracted_data = {}
    all_successful = True

    for table in source_tables:
        # Example: Add specific read options if needed for a table
        # read_opts = {"queryTimeout": "300"} if table == "dbo.LargeTable" else None
        read_opts = None 
        
        success = extract_and_write_bronze(spark, table, process_date, read_options=read_opts)
        if success:
            # Read back the data from bronze to pass to silver (optional, could pass paths)
            try:
                bronze_path = get_bronze_path(table)
                logger.info(f"Reading back Bronze data from: {bronze_path} for date {process_date}")
                df_bronze = spark.read.format("delta").load(bronze_path)
                # Filter for the specific process date if needed (depends on downstream logic)
                extracted_data[table] = df_bronze.where(F.col("_ingestion_date") == F.lit(process_date))
                logger.info(f"Successfully read back {table} from Bronze.")
            except Exception as e:
                 logger.error(f"Failed to read back Bronze data for {table} from {bronze_path}: {e}", exc_info=True)
                 extracted_data[table] = None
                 all_successful = False
        else:
            logger.error(f"Bronze extraction failed for table: {table}")
            extracted_data[table] = None
            all_successful = False

    if not all_successful:
         logger.warning("One or more Bronze extraction steps failed.")

    return extracted_data 