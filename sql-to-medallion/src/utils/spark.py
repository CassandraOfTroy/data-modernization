from pyspark.sql import SparkSession
import os

from .logging import get_logger
from .config import APP_CONFIG

logger = get_logger(__name__)

def get_spark_session(app_name: str = "SQLtoMedallion") -> SparkSession:
    """Gets or creates a SparkSession configured for Azure Databricks and ADLS.

    Args:
        app_name: The name for the Spark application.

    Returns:
        A configured SparkSession instance.
    """
    logger.info(f"Creating or getting SparkSession: {app_name}")

    spark_builder = SparkSession.builder.appName(app_name)

    # --- Delta Lake Configuration ---
    spark_builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                 .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    # --- ADLS Gen2 Configuration ---
    # Prefer using Databricks secrets or instance profiles/managed identity for auth.
    # If using Account Key (less secure, ok for dev):
    adls_config = APP_CONFIG.get('adls', {})
    storage_account_name = adls_config.get('storage_account_name')
    storage_account_key = adls_config.get('storage_account_key')

    if storage_account_name and storage_account_key:
        logger.info(f"Configuring Spark for ADLS Gen2 using Account Key for: {storage_account_name}")
        spark_builder.config(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)
    elif storage_account_name:
         logger.info(f"Configuring Spark for ADLS Gen2: {storage_account_name}. Assuming Managed Identity/Instance Profile/Secret Scope auth.")
         # No explicit key needed if using other auth methods configured in Databricks cluster
         pass
    else:
        logger.warning("ADLS Storage Account Name not configured. ADLS operations might fail.")

    # --- JDBC Driver Configuration (Optional) ---
    # Ensure the SQL Server JDBC driver JAR is available on the Databricks cluster.
    # It's often pre-installed or can be added via cluster libraries.
    # sql_driver = APP_CONFIG.get('sql_server', {}).get('driver')
    # if sql_driver:
    #     spark_builder.config("spark.driver.extraClassPath", f"path/to/mssql-jdbc-xxx.jar") # Adjust path if needed
    #     spark_builder.config("spark.executor.extraClassPath", f"path/to/mssql-jdbc-xxx.jar")

    # --- Build Spark Session ---
    try:
        spark = spark_builder.getOrCreate()
        logger.info("SparkSession created successfully.")
        
        # Set log level on SparkContext too if desired
        # spark.sparkContext.setLogLevel("WARN") # Example: Set to WARN
        
        return spark
    except Exception as e:
        logger.error(f"Error creating SparkSession: {e}", exc_info=True)
        raise

# Example usage:
# if __name__ == "__main__":
#     spark = get_spark_session("MyApp")
#     print(f"Spark version: {spark.version}")
#     # You can now use the 'spark' object
#     spark.stop() 