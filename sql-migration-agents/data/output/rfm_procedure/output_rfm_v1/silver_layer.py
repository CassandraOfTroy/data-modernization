# SILVER LAYER START: Data Transformation
from pyspark.sql.functions import col, sum, avg, min, max, count, when, datediff, lit

# Load Bronze Layer data
customers = spark.read.format("delta").load("/mnt/delta/bronze/customers")
customer_addresses = spark.read.format("delta").load("/mnt/delta/bronze/customer_addresses")
orders = spark.read.format("delta").load("/mnt/delta/bronze/orders")
order_details = spark.read.format("delta").load("/mnt/delta/bronze/order_details")
products = spark.read.format("delta").load("/mnt/delta/bronze/products")
returns = spark.read.format("delta").load("/mnt/delta/bronze/returns")

# Step 1: Extract Customer Base
customer_base = customers.join(customer_addresses, ["CustomerID"], "left").select(
    col("CustomerID"),
    col("FirstName").alias("CustomerName"),
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

# Step 2: Transaction Summary
transaction_summary = orders.join(order_details, ["OrderID"]).groupBy("CustomerID").agg(
    count("OrderID").alias("TotalOrders"),
    sum(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).alias("TotalSpent"),
    avg(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).alias("AvgOrderValue"),
    min("OrderDate").alias("FirstPurchaseDate"),
    max("OrderDate").alias("LastPurchaseDate"),
    datediff(lit("2023-01-01"), max("OrderDate")).alias("DaysSinceLastPurchase")
)

# Write Silver Layer data
customer_base.write.format("delta").mode("overwrite").save("/mnt/delta/silver/customer_base")
transaction_summary.write.format("delta").mode("overwrite").save("/mnt/delta/silver/transaction_summary")
# SILVER LAYER END