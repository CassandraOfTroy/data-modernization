# GOLD LAYER START
from pyspark.sql.functions import when, col

# Load Silver Layer data
rfm_scores = spark.read.format("delta").load("/silver/rfm_scores")
transaction_summary = spark.read.format("delta").load("/silver/transaction_summary")
customer_base = spark.read.format("delta").load("/silver/customer_base")

# Customer Segmentation
customer_segments = rfm_scores.join(transaction_summary, "CustomerID") \
    .join(customer_base, "CustomerID") \
    .select(
        col("CustomerID"),
        when(col("TotalSpent") >= 5000, "High Value")
        .when(col("TotalSpent") >= 2500, "Medium Value")
        .otherwise("Low Value").alias("ValueSegment"),
        when(col("RFMScore") >= 13, "Champions")
        .when(col("RecencyScore") >= 4, "Loyal Customers")
        .otherwise("Others").alias("BehaviorSegment"),
        when(col("TotalOrders") == 1, "New Customer")
        .when(col("DaysSinceLastPurchase") <= 30, "Active")
        .otherwise("Inactive").alias("LifecycleSegment")
    )

# Save Customer Segments to Gold Layer
customer_segments.write.format("delta").mode("overwrite").save("/gold/customer_segments")
# GOLD LAYER END