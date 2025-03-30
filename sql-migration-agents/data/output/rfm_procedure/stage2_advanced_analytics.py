# STAGE 2: ADVANCED ANALYTICS START
from pyspark.sql.functions import sum, count, avg, min, max, datediff, lit, ntile
from pyspark.sql.window import Window

# Load Silver Layer data
orders = spark.read.format("delta").load("/bronze/orders")
order_details = spark.read.format("delta").load("/bronze/order_details")
returns = spark.read.format("delta").load("/bronze/returns")
customer_base = spark.read.format("delta").load("/silver/customer_base")

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
transaction_summary.write.format("delta").mode("overwrite").save("/silver/transaction_summary")

# Calculate RFM Scores
rfm_scores = transaction_summary.withColumn(
    "RecencyScore", ntile(5).over(Window.orderBy(col("DaysSinceLastPurchase").asc()))
).withColumn(
    "FrequencyScore", ntile(5).over(Window.orderBy(col("TotalOrders").desc()))
).withColumn(
    "MonetaryScore", ntile(5).over(Window.orderBy(col("TotalSpent").desc()))
).withColumn(
    "RFMScore", col("RecencyScore") + col("FrequencyScore") + col("MonetaryScore")
)

# Save RFM Scores to Silver Layer
rfm_scores.write.format("delta").mode("overwrite").save("/silver/rfm_scores")
# STAGE 2: ADVANCED ANALYTICS END