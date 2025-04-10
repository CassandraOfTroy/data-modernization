```python
# STAGE 2: ADVANCED ANALYTICS START

# Calculate RFM Scores
rfm_scores_df = (
    transaction_summary_df.alias("ts")
    .select(
        "CustomerID",
        F.ntile(5).over(Window.orderBy(F.col("DaysSinceLastPurchase"))).alias("RecencyScore"),
        F.ntile(5).over(Window.orderBy(F.col("TotalOrders").desc())).alias("FrequencyScore"),
        F.ntile(5).over(Window.orderBy(F.col("TotalSpent").desc())).alias("MonetaryScore")
    )
)

# Create Customer Metrics DataFrame
customer_metrics_df = (
    rfm_scores_df.alias("rfm")
    .select(
        "CustomerID",
        "RecencyScore",
        "FrequencyScore",
        "MonetaryScore",
        (F.col("RecencyScore") + F.col("FrequencyScore") + F.col("MonetaryScore")).alias("RFMScore")
    )
)

# STAGE 2: ADVANCED ANALYTICS END
```