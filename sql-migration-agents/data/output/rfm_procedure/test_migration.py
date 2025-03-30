# BRONZE LAYER START: Raw Data Ingestion
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerAnalytics").getOrCreate()

# Utility function to load SQL table
def load_table_from_sql(table_name, jdbc_url, username, password):
    return spark.read.format("jdbc").options(
        url=jdbc_url,
        dbtable=table_name,
        user=username,
        password=password
    ).load()

# Load raw data from SQL tables
jdbc_url = "jdbc:sqlserver://<server>;database=<db>"
username = "<username>"
password = "<password>"

customers = load_table_from_sql("dbo.Customers", jdbc_url, username, password)
customer_addresses = load_table_from_sql("dbo.CustomerAddresses", jdbc_url, username, password)
customer_interactions = load_table_from_sql("dbo.CustomerInteractions", jdbc_url, username, password)
orders = load_table_from_sql("dbo.Orders", jdbc_url, username, password)
order_details = load_table_from_sql("dbo.OrderDetails", jdbc_url, username, password)
products = load_table_from_sql("dbo.Products", jdbc_url, username, password)
returns = load_table_from_sql("dbo.Returns", jdbc_url, username, password)

# Utility function to save data to Delta Lake
def save_to_delta(df, path, partition_col=None):
    if partition_col:
        df.write.format("delta").partitionBy(partition_col).mode("overwrite").save(path)
    else:
        df.write.format("delta").mode("overwrite").save(path)

# Save raw data to Bronze Layer
save_to_delta(customers, "/bronze/customers")
save_to_delta(customer_addresses, "/bronze/customer_addresses")
save_to_delta(customer_interactions, "/bronze/customer_interactions")
save_to_delta(orders, "/bronze/orders")
save_to_delta(order_details, "/bronze/order_details")
save_to_delta(products, "/bronze/products")
save_to_delta(returns, "/bronze/returns")
# BRONZE LAYER END

# SILVER LAYER START: Data Transformation
from pyspark.sql.functions import col, lit, when, max, concat_ws, sum, count, avg, min, datediff
from pyspark.sql.window import Window

# Load Bronze Layer data
customers = spark.read.format("delta").load("/bronze/customers")
customer_addresses = spark.read.format("delta").load("/bronze/customer_addresses")
customer_interactions = spark.read.format("delta").load("/bronze/customer_interactions")
orders = spark.read.format("delta").load("/bronze/orders")
order_details = spark.read.format("delta").load("/bronze/order_details")

# Create Customer Base
customer_base = customers.join(
    customer_addresses.filter(col("AddressType") == "Primary"),
    "CustomerID",
    "left"
).join(
    customer_interactions.groupBy("CustomerID").agg(max("ContactDate").alias("LastContactDate")),
    "CustomerID",
    "left"
).select(
    col("CustomerID"),
    concat_ws(" ", col("FirstName"), col("LastName")).alias("CustomerName"),
    col("Email"),
    col("Phone"),
    col("StreetAddress").alias("Address"),
    col("PostalCode"),
    col("City"),
    col("State"),
    col("Country"),
    col("CustomerType"),
    col("AccountManagerID").alias("AccountManager"),
    col("CreatedDate"),
    col("ModifiedDate"),
    when(col("Status") == "Active", lit(1)).otherwise(lit(0)).alias("IsActive"),
    col("LastContactDate")
)

# Save Customer Base to Silver Layer
save_to_delta(customer_base, "/silver/customer_base")

# Calculate Transaction Summary
transaction_summary = orders.join(order_details, "OrderID") \
    .groupBy("CustomerID") \
    .agg(
        count("OrderID").alias("TotalOrders"),
        sum(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).alias("TotalSpent"),
        min("OrderDate").alias("FirstPurchaseDate"),
        max("OrderDate").alias("LastPurchaseDate")
    ).withColumn(
        "AvgOrderValue", col("TotalSpent") / col("TotalOrders")
    ).withColumn(
        "DaysSinceLastPurchase", datediff(lit("<DataDate>"), col("LastPurchaseDate"))
    )

# Save Transaction Summary to Silver Layer
save_to_delta(transaction_summary, "/silver/transaction_summary")
# SILVER LAYER END

# GOLD LAYER START: Customer Segmentation
from pyspark.sql.functions import when

# Load Silver Layer data
transaction_summary = spark.read.format("delta").load("/silver/transaction_summary")
customer_base = spark.read.format("delta").load("/silver/customer_base")

# Customer Segmentation
customer_segments = transaction_summary.join(customer_base, "CustomerID") \
    .select(
        col("CustomerID"),
        when(col("TotalSpent") >= 5000, "High Value")
        .when(col("TotalSpent") >= 2500, "Medium Value")
        .otherwise("Low Value").alias("ValueSegment"),
        when(col("TotalOrders") >= 10, "Champions")
        .when(col("TotalOrders") >= 5, "Loyal Customers")
        .otherwise("Others").alias("BehaviorSegment"),
        when(col("TotalOrders") == 1, "New Customer")
        .when(col("DaysSinceLastPurchase") <= 30, "Active")
        .otherwise("Inactive").alias("LifecycleSegment")
    )

# Save Customer Segments to Gold Layer
save_to_delta(customer_segments, "/gold/customer_segments", partition_col="LifecycleSegment")
# GOLD LAYER END

# RETENTION POLICY START
from delta.tables import DeltaTable

def apply_retention_policy(delta_path, retention_date):
    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.delete(f"ProcessDate < '{retention_date}'")

# Apply retention policy to Gold Layer
apply_retention_policy("/gold/customer_segments", "<RetentionDate>")
# RETENTION POLICY END