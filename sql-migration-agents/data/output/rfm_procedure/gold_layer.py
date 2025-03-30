# GOLD LAYER START: Customer Segmentation
from pyspark.sql.functions import when, lit

# Load Silver Layer data
customer_base = spark.read.format("delta").load("/mnt/delta/silver/customer_base")
transaction_summary = spark.read.format("delta").load("/mnt/delta/silver/transaction_summary")

# Customer Segmentation
customer_segments = transaction_summary.join(customer_base, ["CustomerID"]).select(
    col("CustomerID"),
    when(col("TotalSpent") >= 5000, "High Value").when(col("TotalSpent") >= 2500, "Medium Value").otherwise("Low Value").alias("ValueSegment"),
    when(col("TotalOrders") >= 10, "Champions").when(col("TotalOrders") >= 5, "Loyal Customers").otherwise("Others").alias("BehaviorSegment"),
    when(col("DaysSinceLastPurchase") <= 30, "New Customer").when(col("DaysSinceLastPurchase") <= 90, "Active").otherwise("Inactive").alias("LifecycleSegment")
)

# Write Gold Layer data
customer_segments.write.format("delta").mode("overwrite").save("/mnt/delta/gold/customer_segments")
# GOLD LAYER END