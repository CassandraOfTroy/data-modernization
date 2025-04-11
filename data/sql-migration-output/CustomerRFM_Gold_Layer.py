```python
# GOLD LAYER START

# Final Customer Analytics DataFrame
customer_analytics_df = (
    customer_base_df.alias("cb")
    .join(transaction_summary_df.alias("ts"), "CustomerID", "left")
    .join(customer_metrics_df.alias("cm"), "CustomerID", "left")
    .select(
        "cb.CustomerID",
        "cb.CustomerName",
        "cb.Email",
        "ts.TotalOrders",
        "ts.TotalSpent",
        "cm.RFMScore"
    )
)

# Save the final DataFrame to the target table
customer_analytics_df.write.mode("overwrite").saveAsTable("dbo.CustomerAnalytics")

# GOLD LAYER END
```