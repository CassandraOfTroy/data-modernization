# STAGE 1: BASE DATA START
# -------------------------------------------------------------
# This stage creates intermediate DataFrames replicating #CustomerBase
# and #TransactionSummary from the SQL procedure.

# 1) Create #CustomerBase equivalent
customerBaseDF = (
    customersBronzeDF.alias("c")
    .join(
        addressesBronzeDF.alias("a"),
        (F.col("c.CustomerID") == F.col("a.CustomerID")) & (F.col("a.AddressType") == "Primary"),
        "left"
    )
    .join(
        interactionsBronzeDF.alias("i"),
        F.col("c.CustomerID") == F.col("i.CustomerID"),
        "left"
    )
    .where(
        (processType == 'FULL')
        | (F.col("c.ModifiedDate") >= F.lit(dataDate))
        | (F.col("i.ContactDate") >= F.lit(dataDate))
    )
    .groupBy(
        "c.CustomerID",
        "c.FirstName",
        "c.LastName",
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
        "c.Status"
    )
    .agg(F.max("i.ContactDate").alias("LastContactDate"))
    .select(
        F.col("CustomerID"),
        F.concat_ws(" ", F.col("FirstName"), F.col("LastName")).alias("CustomerName"),
        F.col("Email"),
        F.col("Phone"),
        F.col("StreetAddress").alias("Address"),
        F.col("PostalCode"),
        F.col("City"),
        F.col("State"),
        F.col("Country"),
        F.col("CustomerType"),
        F.col("AccountManagerID").alias("AccountManager"),
        F.col("CreatedDate"),
        F.col("ModifiedDate"),
        (F.col("Status") == "Active").cast("boolean").alias("IsActive"),
        F.col("LastContactDate")
    )
)

# 2) Create #TransactionSummary equivalent
# 2a) Order Summaries
orderSummaryDF = (
    ordersBronzeDF.alias("o")
    .join(orderDetailsBronzeDF.alias("od"), "OrderID")
    .where(F.col("o.Status") == "Completed")
    .groupBy("o.CustomerID")
    .agg(
        F.countDistinct("o.OrderID").alias("TotalOrders"),
        F.sum(F.col("od.Quantity") * F.col("od.UnitPrice") * (1 - F.col("od.Discount"))).alias("TotalSpent"),
        F.min("o.OrderDate").alias("FirstPurchaseDate"),
        F.max("o.OrderDate").alias("LastPurchaseDate")
    )
)

# 2b) Join with #CustomerBase subset
transactionSummaryDF = (
    orderSummaryDF.alias("os")
    .join(
        customerBaseDF.select("CustomerID").alias("cb"),
        F.col("os.CustomerID") == F.col("cb.CustomerID"),
        "inner"
    )
    .select(
        F.col("os.CustomerID"),
        F.col("os.TotalOrders"),
        F.col("os.TotalSpent"),
        F.when(F.col("os.TotalOrders") > 0, F.col("os.TotalSpent") / F.col("os.TotalOrders"))
         .otherwise(F.lit(0.0)).alias("AvgOrderValue"),
        F.col("os.FirstPurchaseDate"),
        F.col("os.LastPurchaseDate"),
        F.datediff(F.lit(dataDate), F.col("os.LastPurchaseDate")).alias("DaysSinceLastPurchase")
    )
)

# STAGE 1: BASE DATA END