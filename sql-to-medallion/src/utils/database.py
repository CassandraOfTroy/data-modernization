from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, Optional

from .config import APP_CONFIG
from .logging import get_logger

logger = get_logger(__name__)

def read_sql_server_table(
    spark: SparkSession,
    table_name: str,
    options: Optional[Dict[str, str]] = None
) -> Optional[DataFrame]:
    """Reads data from a specified SQL Server table using JDBC.

    Args:
        spark: The active SparkSession.
        table_name: The name of the table to read (e.g., 'dbo.Customers').
        options: Additional JDBC options (e.g., query timeout, fetch size).

    Returns:
        A Spark DataFrame containing the table data, or None if an error occurs.
    """
    sql_config = APP_CONFIG.get('sql_server', {})
    host = sql_config.get('host')
    database = sql_config.get('database')
    user = sql_config.get('user')
    password = sql_config.get('password')
    # driver = sql_config.get('driver') # Optional: Usually handled by Spark/Databricks

    if not all([host, database, user, password]):
        logger.error("SQL Server connection details missing in configuration.")
        return None

    jdbc_url = f"jdbc:sqlserver://{host};databaseName={database};encrypt=true;trustServerCertificate=false;"
    # Note: Production environments might require different security settings
    # like integratedSecurity=true, authentication=ActiveDirectoryPassword, etc.
    # Adjust trustServerCertificate based on your server setup.

    connection_properties = {
        "user": user,
        "password": password,
        # "driver": driver # Only specify if needed and Jar is configured
    }

    # Merge custom options
    if options:
        connection_properties.update(options)

    logger.info(f"Reading data from SQL Server table: {table_name} on host: {host}")
    
    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=table_name,
            properties=connection_properties
        )
        logger.info(f"Successfully read {df.count()} rows from {table_name}.")
        return df
    except Exception as e:
        logger.error(f"Error reading SQL Server table {table_name}: {e}", exc_info=True)
        # Depending on requirements, you might want to raise the exception
        # raise
        return None 