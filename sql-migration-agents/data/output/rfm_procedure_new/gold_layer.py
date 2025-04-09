# GOLD LAYER START: Perform customer segmentation and persist final analytics
customer_segments = rfm_scores.withColumn(
    "ValueSegment", when(col("TotalSpent") >= 5000, "High Value")
                    .when(col("TotalSpent") >= 2500, "Medium Value")
                    .otherwise("Low Value")
).withColumn(
    "BehaviorSegment", when(col("RFMScore") >= 13, "Champions")
                       .when((col("RecencyScore") >= 4) & (col("FrequencyScore") >= 3), "Loyal Customers")
                       .when((col("RecencyScore") >= 4) & (col("FrequencyScore") <= 2), "Potential Loyalists")
                       .when((col("RecencyScore") <= 2) & (col("FrequencyScore") >= 3), "At Risk")
                       .when((col("RecencyScore") <= 2) & (col("FrequencyScore") <= 2), "Hibernating")
                       .otherwise("Others")
)

# Write customer segments to Delta Lake
customer_segments.write.format("delta").mode("overwrite").save("/gold/customer_segments")
# GOLD LAYER END