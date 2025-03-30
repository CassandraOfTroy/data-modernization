# BRONZE LAYER START: Raw Data Ingestion
from pyspark.sql import SparkSession

# Load raw data from SQL Server into Delta Lake (Bronze Layer)
def load_table_to_bronze(table_name, save_path):
    df = spark.read.format("jdbc").options(
        url="jdbc:sqlserver://<server>:<port>;databaseName=<database>",
        dbtable=table_name,
        user="<username>",
        password="<password>"
    ).load()
    df.write.format("delta").mode("overwrite").save(save_path)

# Ingest tables into Bronze Layer
load_table_to_bronze("dbo.Customers", "/mnt/delta/bronze/customers")
load_table_to_bronze("dbo.CustomerAddresses", "/mnt/delta/bronze/customer_addresses")
load_table_to_bronze("dbo.Orders", "/mnt/delta/bronze/orders")
load_table_to_bronze("dbo.OrderDetails", "/mnt/delta/bronze/order_details")
load_table_to_bronze("dbo.Products", "/mnt/delta/bronze/products")
load_table_to_bronze("dbo.Returns", "/mnt/delta/bronze/returns")
# BRONZE LAYER END