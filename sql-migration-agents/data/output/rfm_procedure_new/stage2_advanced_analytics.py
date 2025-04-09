# STAGE 2: ADVANCED ANALYTICS START: Calculate RFM metrics and customer segmentation
from pyspark.sql.functions import when

# Load data from Silver Layer
transaction_summary_silver = spark.read.format("delta").load("/silver/transaction_summary")

# Calculate RFM scores
rfm_scores = transaction_summary_silver.withColumn(
    "RecencyScore", when(col("DaysSinceLastPurchase") <= 30, 5)
                    .when(col("DaysSinceLastPurchase") <= 60, 4)
                    .when(col("DaysSinceLastPurchase") <= 90, 3)
                    .when(col("DaysSinceLastPurchase") <= 180, 2)
                    .otherwise(1)
).withColumn(
    "FrequencyScore", when(col("TotalOrders") >= 10, 5)
                      .when(col("TotalOrders") >= 5, 4)
                      .when(col("TotalOrders") >= 3, 3)
                      .when(col("TotalOrders") >= 1, 2)
                      .otherwise(1)
).withColumn(
    "MonetaryScore", when(col("TotalSpent") >= 5000, 5)
                     .when(col("TotalSpent") >= 2500, 4)
                     .when(col("TotalSpent") >= 1000, 3)
                     .when(col("TotalSpent") >= 500, 2)
                     .otherwise(1)
).withColumn(
    "RFMScore", col("RecencyScore") + col("FrequencyScore") + col("MonetaryScore")
)

# Write RFM scores to Delta Lake
rfm_scores.write.format("delta").mode("overwrite").save("/gold/rfm_scores")
# STAGE 2: ADVANCED ANALYTICS END