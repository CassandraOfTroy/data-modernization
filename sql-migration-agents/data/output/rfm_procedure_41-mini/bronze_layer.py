# BRONZE LAYER START
# ------------------------------------------------------------------------
# This block ingests raw data from the SQL source into Bronze tables.
# Adapt as needed for your actual data sources (e.g., JDBC, CSV, Parquet).

# Example parameters (these could be read from config or passed to the notebook):
data_date = "2023-10-01"
process_type = "FULL"  # or "INCREMENTAL"
debug_mode = False
retention_period_days = 365
high_value_threshold = 5000.00
loyalty_threshold = 5
churn_days_very_high = 180
churn_days_high = 90
churn_days_medium = 60
churn_days_low = 30

# Example: reading from SQL Server into a DataFrame via JDBC, then writing to bronze.
jdbc_url = "jdbc:sqlserver://<YOUR_SERVER>;databaseName=<YOUR_DB>"
connection_props = {"user": "<USERNAME>", "password": "<PASSWORD>"}

source_tables = {
    "Customers": "dbo.Customers",
    "CustomerAddresses": "dbo.CustomerAddresses",
    "CustomerInteractions": "dbo.CustomerInteractions",
    "Orders": "dbo.Orders",
    "OrderDetails": "dbo.OrderDetails",
    "Products": "dbo.Products",
    "Returns": "dbo.Returns"
}

for alias, table_name in source_tables.items():
    # Read from SQL
    df = (spark.read
               .format("jdbc")
               .option("url", jdbc_url)
               .option("dbtable", table_name)
               .options(**connection_props)
               .load()
         )

    # Write to Bronze layer (Delta)
    bronze_path = f"/lakehouse/bronze/{alias}"
    df.write.format("delta").mode("overwrite").save(bronze_path)

    # Register the Bronze table in Fabric's metastore (if not already)
    spark.sql(f"CREATE TABLE IF NOT EXISTS bronze.{alias} "
              f"USING DELTA LOCATION '{bronze_path}'")

print("Bronze layer ingestion complete.")