```python
# STAGE 1: BASE DATA START

# Create a DataFrame for Transaction Summary
from pyspark.sql import Window
from pyspark.sql.functions import countDistinct, sum, min, max, datediff, current_date

# Load Orders and OrderDetails data
orders_df = spark.read.format("delta").load("path_to_orders")
order_details_df = spark.read.format("delta").load("path_to_order_details")

# Calculate Transaction Summary
order_summary_df = orders_df.alias("o")
    .join(order_details_df.alias("od"), col("o.OrderID") == col("od.OrderID"))
    .filter(col("o.Status") == "Completed")
    .groupBy("o.CustomerID")
    .agg(
        countDistinct("o.OrderID").alias("TotalOrders"),
        sum((col("od.Quantity") * col("od.UnitPrice") * (1 - col("od.Discount"))).alias("TotalSpent")),
        min("o.OrderDate").alias("FirstPurchaseDate"),
        max("o.OrderDate").alias("LastPurchaseDate")
    )

# Create Transaction Summary DataFrame
transaction_summary_df = order_summary_df.select(
    "CustomerID",
    "TotalOrders",
    "TotalSpent",
    (when(col("TotalOrders") > 0, col("TotalSpent") / col("TotalOrders")).otherwise(0)).alias("AvgOrderValue"),
    "FirstPurchaseDate",
    "LastPurchaseDate",
    datediff(current_date(), col("LastPurchaseDate")).alias("DaysSinceLastPurchase"),
    lit(None).alias("TopCategory"),
    lit(None).alias("TopProduct"),
    lit(0).alias("ReturnRate")
)
```