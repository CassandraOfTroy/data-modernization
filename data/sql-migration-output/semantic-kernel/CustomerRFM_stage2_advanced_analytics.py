```python
# STAGE 2: ADVANCED ANALYTICS START

from pyspark.sql import Window
from pyspark.sql.functions import ntile

# Calculate RFM Scores
rfm_scores_df = transaction_summary_df.select(
    "CustomerID",
    ntile(5).over(Window.orderBy("DaysSinceLastPurchase")).alias("RecencyScore"),
    ntile(5).over(Window.orderBy(col("TotalOrders").desc())).alias("FrequencyScore"),
    ntile(5).over(Window.orderBy(col("TotalSpent").desc())).alias("MonetaryScore"
)

# Calculate RFM Score
rfm_scores_df = rfm_scores_df.withColumn(
    "RFMScore",
    col("RecencyScore") + col("FrequencyScore") + col("MonetaryScore")
)

# Create Customer Metrics DataFrame
customer_metrics_df = rfm_scores_df.select(
    "CustomerID",
    "RecencyScore",
    "FrequencyScore",
    "MonetaryScore",
    "RFMScore"
)
```