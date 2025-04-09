# STAGE 1: BASE DATA START
# ========================

# Create #CustomerBase equivalent
df_customerBase = (
    df_customers_bronze.alias("c")
    .join(
        df_addresses_bronze.alias("a"),
        (F.col("c.CustomerID") == F.col("a.CustomerID")) & (F.col("a.AddressType") == F.lit("Primary")),
        "left"
    )
    .join(
        df_interactions_bronze.alias("i"),
        F.col("c.CustomerID") == F.col("i.CustomerID"),
        "left"
    )
    # Filter logic: FULL or incremental changes (modified date or contact date)
    .where(
        (process_type == "FULL")
        | (F.col("c.ModifiedDate") >= process_date)
        | (F.col("i.ContactDate") >= process_date)
    )
    .groupBy(
        "c.CustomerID", "c.FirstName", "c.LastName", "c.Email", "c.Phone",
        "a.StreetAddress", "a.PostalCode", "a.City", "a.State", "a.Country",
        "c.CustomerType", "c.AccountManagerID", "c.CreatedDate", "c.ModifiedDate", "c.Status"
    )
    .agg(F.max("i.ContactDate").alias("LastContactDate"))
    .select(
        F.col("c.CustomerID"),
        F.concat_ws(" ", F.col("c.FirstName"), F.col("c.LastName")).alias("CustomerName"),
        "c.Email",
        "c.Phone",
        F.col("a.StreetAddress").alias("Address"),
        "a.PostalCode",
        "a.City",
        "a.State",
        "a.Country",
        "c.CustomerType",
        F.col("c.AccountManagerID").alias("AccountManager"),
        "c.CreatedDate",
        "c.ModifiedDate",
        F.when(F.col("c.Status") == "Active", F.lit(1)).otherwise(F.lit(0)).alias("IsActive"),
        "LastContactDate"
    )
)

# Create #TransactionSummary equivalent
df_orders_completed = df_orders_bronze.filter(F.col("Status") == "Completed")

df_orderJoin = (
    df_orders_completed.alias("o")
    .join(
        df_orderdetails_bronze.alias("od"),
        F.col("o.OrderID") == F.col("od.OrderID"),
        "inner"
    )
)

df_orderSummary = (
    df_orderJoin
    .groupBy("o.CustomerID")
    .agg(
        F.countDistinct("o.OrderID").alias("TotalOrders"),
        F.sum(F.col("od.Quantity") * F.col("od.UnitPrice") * (1 - F.col("od.Discount"))).alias("TotalSpent"),
        F.min("o.OrderDate").alias("FirstPurchaseDate"),
        F.max("o.OrderDate").alias("LastPurchaseDate")
    )
)

df_transactionSummary = (
    df_orderSummary.alias("os")
    # Filter to only those in #CustomerBase
    .join(df_customerBase.select("CustomerID"), "CustomerID", "inner")
    .select(
        "os.CustomerID",
        "os.TotalOrders",
        "os.TotalSpent",
        F.when(F.col("os.TotalOrders") > 0, F.col("os.TotalSpent") / F.col("os.TotalOrders"))
         .otherwise(F.lit(0)).alias("AvgOrderValue"),
        "os.FirstPurchaseDate",
        "os.LastPurchaseDate",
        F.datediff(process_date, F.col("os.LastPurchaseDate")).alias("DaysSinceLastPurchase")
    )
)

# Optionally write out to a Silver location
# df_customerBase.write.format("delta").mode("overwrite").save("Tables/Silver/CustomerBase")
# df_transactionSummary.write.format("delta").mode("overwrite").save("Tables/Silver/TransactionSummary")

# STAGE 1: BASE DATA END