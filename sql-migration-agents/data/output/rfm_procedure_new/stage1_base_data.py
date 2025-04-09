# STAGE 1: BASE DATA START: Create intermediate DataFrames for CustomerBase and TransactionSummary
from pyspark.sql.functions import col, lit, max, countDistinct, sum, avg, min, datediff, current_date

# Load raw data from Bronze Layer
customers_bronze = spark.read.format("delta").load("/bronze/customers")
customer_addresses_bronze = spark.read.format("delta").load("/bronze/customer_addresses")
customer_interactions_bronze = spark.read.format("delta").load("/bronze/customer_interactions")
orders_bronze = spark.read.format("delta").load("/bronze/orders")
order_details_bronze = spark.read.format("delta").load("/bronze/order_details")

# Create CustomerBase DataFrame
customer_base = customers_bronze.join(
    customer_addresses_bronze.filter(col("AddressType") == "Primary"),
    customers_bronze["CustomerID"] == customer_addresses_bronze["CustomerID"],
    "left"
).join(
    customer_interactions_bronze,
    customers_bronze["CustomerID"] == customer_interactions_bronze["CustomerID"],
    "left"
).groupBy(
    customers_bronze["CustomerID"]
).agg(
    max("ContactDate").alias("LastContactDate")
).select(
    customers_bronze["CustomerID"],
    (customers_bronze["FirstName"] + " " + customers_bronze["LastName"]).alias("CustomerName"),
    customers_bronze["Email"],
    customers_bronze["Phone"],
    customer_addresses_bronze["StreetAddress"].alias("Address"),
    customer_addresses_bronze["PostalCode"],
    customer_addresses_bronze["City"],
    customer_addresses_bronze["State"],
    customer_addresses_bronze["Country"],
    customers_bronze["CustomerType"],
    customers_bronze["AccountManagerID"].alias("AccountManager"),
    customers_bronze["CreatedDate"],
    customers_bronze["ModifiedDate"],
    (lit(1).when(customers_bronze["Status"] == "Active").otherwise(0)).alias("IsActive"),
    col("LastContactDate")
)

# Write CustomerBase to Delta Lake
customer_base.write.format("delta").mode("overwrite").save("/silver/customer_base")

# Create TransactionSummary DataFrame
transaction_summary = orders_bronze.join(order_details_bronze, "OrderID") \
    .groupBy("CustomerID").agg(
        countDistinct("OrderID").alias("TotalOrders"),
        sum(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).alias("TotalSpent"),
        avg(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).alias("AvgOrderValue"),
        min("OrderDate").alias("FirstPurchaseDate"),
        max("OrderDate").alias("LastPurchaseDate"),
        datediff(current_date(), max("OrderDate")).alias("DaysSinceLastPurchase")
    )

# Write TransactionSummary to Delta Lake
transaction_summary.write.format("delta").mode("overwrite").save("/silver/transaction_summary")
# STAGE 1: BASE DATA END