# SILVER LAYER START
from pyspark.sql.functions import col, lit, when, max, count, sum, avg, min, datediff, ntile
from pyspark.sql.window import Window

# Extract Customer Base
customer_base = customers.join(customer_addresses, ["CustomerID"], "left") \
    .join(customer_interactions, ["CustomerID"], "left") \
    .select(
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
        max(col("ContactDate")).alias("LastContactDate")
    )

customer_base.write.format("delta").mode("overwrite").save("fabric://silver/customer_base")

# Process Transaction Summary
order_summary = orders.join(order_details, ["OrderID"]) \
    .groupBy("CustomerID") \
    .agg(
        count("OrderID").alias("TotalOrders"),
        sum(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).alias("TotalSpent"),
        min("OrderDate").alias("FirstPurchaseDate"),
        max("OrderDate").alias("LastPurchaseDate")
    )

transaction_summary = order_summary.withColumn(
    "AvgOrderValue", when(col("TotalOrders") > 0, col("TotalSpent") / col("TotalOrders")).otherwise(0)
).withColumn(
    "DaysSinceLastPurchase", datediff(lit("2023-10-01"), col("LastPurchaseDate"))
)

transaction_summary.write.format("delta").mode("overwrite").save("fabric://silver/transaction_summary")
# SILVER LAYER END