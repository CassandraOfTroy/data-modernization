```python
# GOLD LAYER START

# Final Customer Analytics DataFrame
final_customer_analytics_df = customer_metrics_df.join(transaction_summary_df,
    "CustomerID")
    .select(
        "CustomerID",
        "RecencyScore",
        "FrequencyScore",
        "MonetaryScore",
        "RFMScore",
        "TotalOrders",
        "TotalSpent",
        "AvgOrderValue",
        "FirstPurchaseDate",
        "LastPurchaseDate",
        "DaysSinceLastPurchase"
    )

# Save final results to Delta Lake
final_customer_analytics_df.write.format("delta").mode("overwrite").save("path_to_final_customer_analytics")
```