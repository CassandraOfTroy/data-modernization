# GOLD LAYER START
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

rfm_scores.write.format("delta").mode("overwrite").save("fabric://gold/rfm_scores")

# Customer Segmentation
customer_segments = rfm_scores.withColumn(
    "ValueSegment", when(col("TotalSpent") >= 5000, "High Value").otherwise("Low Value")
).withColumn(
    "BehaviorSegment", when(col("RFMScore") >= 13, "Champions").otherwise("Others")
)

customer_segments.write.format("delta").mode("overwrite").save("fabric://gold/customer_segments")
# GOLD LAYER END