# BRONZE LAYER START: Ingest raw data from SQL Server into Delta Lake
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerAnalytics").getOrCreate()

# JDBC connection properties
jdbc_url = "jdbc:sqlserver://<server>;database=<database>"
connection_properties = {
    "user": "<username>",
    "password": "<password>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Load raw data from SQL Server
customers_df = spark.read.jdbc(url=jdbc_url, table="dbo.Customers", properties=connection_properties)
customer_addresses_df = spark.read.jdbc(url=jdbc_url, table="dbo.CustomerAddresses", properties=connection_properties)
customer_interactions_df = spark.read.jdbc(url=jdbc_url, table="dbo.CustomerInteractions", properties=connection_properties)
orders_df = spark.read.jdbc(url=jdbc_url, table="dbo.Orders", properties=connection_properties)
order_details_df = spark.read.jdbc(url=jdbc_url, table="dbo.OrderDetails", properties=connection_properties)
products_df = spark.read.jdbc(url=jdbc_url, table="dbo.Products", properties=connection_properties)
returns_df = spark.read.jdbc(url=jdbc_url, table="dbo.Returns", properties=connection_properties)

# Write raw data to Delta Lake (Bronze Layer)
customers_df.write.format("delta").mode("overwrite").save("/bronze/customers")
customer_addresses_df.write.format("delta").mode("overwrite").save("/bronze/customer_addresses")
customer_interactions_df.write.format("delta").mode("overwrite").save("/bronze/customer_interactions")
orders_df.write.format("delta").mode("overwrite").save("/bronze/orders")
order_details_df.write.format("delta").mode("overwrite").save("/bronze/order_details")
products_df.write.format("delta").mode("overwrite").save("/bronze/products")
returns_df.write.format("delta").mode("overwrite").save("/bronze/returns")
# BRONZE LAYER END