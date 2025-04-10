```python
# STAGE 1: BASE DATA START

from pyspark.sql import functions as F
from pyspark.sql import Window

# Create Customer Base DataFrame
customer_base_df = (
    customers_df.alias("c")
    .join(customer_addresses_df.alias("a"), (F.col("c.CustomerID") == F.col("a.CustomerID")) & (F.col("a.AddressType") == "Primary"), "left")
    .join(customer_interactions_df.alias("i"), "CustomerID", "left")
    .select(
        F.col("c.CustomerID"),
        (F.concat(F.col("c.FirstName"), F.lit(" "), F.col("c.LastName")).alias("CustomerName")),
        "c.Email",
        "c.Phone",
        "a.StreetAddress",
        "a.PostalCode",
        "a.City",
        "a.State",
        "a.Country",
        "c.CustomerType",
        "c.AccountManagerID",
        "c.CreatedDate",
        "c.ModifiedDate",
        (F.when(F.col("c.Status") == "Active", 1).otherwise(0)).alias("IsActive"),
        F.max("i.ContactDate").alias("LastContactDate")
    )
    .groupBy(
        "c.CustomerID", "c.FirstName", "c.LastName", "c.Email", "c.Phone",
        "a.StreetAddress", "a.PostalCode", "a.City", "a.State", "a.Country",
        "c.CustomerType", "c.AccountManagerID", "c.CreatedDate", "c.ModifiedDate", "c.Status"
    )
)

# Create Transaction Summary DataFrame
order_summary_df = (
    orders_df.alias("o")
    .join(order_details_df.alias("od"), "OrderID")
    .filter(F.col("o.Status") == "Completed")
    .groupBy("o.CustomerID")
    .agg(
        F.countDistinct("o.OrderID").alias("TotalOrders"),
        F.sum(od.Quantity * od.UnitPrice * (1 - od.Discount)).alias("TotalSpent"),
        F.min("o.OrderDate").alias("FirstPurchaseDate"),
        F.max("o.OrderDate").alias("LastPurchaseDate")
    )
)

# Join with Customer Base to create Transaction Summary
transaction_summary_df = (
    order_summary_df.alias("os")
    .join(customer_base_df.alias("cb"), "CustomerID")
    .select(
        "os.CustomerID",
        "os.TotalOrders",
        "os.TotalSpent",
        (F.when(F.col("os.TotalOrders") > 0, F.col("os.TotalSpent") / F.col("os.TotalOrders")).otherwise(0)).alias("AvgOrderValue"),
        "os.FirstPurchaseDate",
        "os.LastPurchaseDate",
        (F.datediff(F.current_date(), "os.LastPurchaseDate").alias("DaysSinceLastPurchase")),
        F.lit(None).alias("TopCategory"),
        F.lit(None).alias("TopProduct"),
        F.lit(0).alias("ReturnRate")
    )
)

# STAGE 1: BASE DATA END
```