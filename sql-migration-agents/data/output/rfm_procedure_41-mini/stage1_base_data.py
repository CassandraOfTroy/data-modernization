# STAGE 1: BASE DATA START
# ------------------------------------------------------------------------
# This block creates intermediate DataFrames equivalent to
# T-SQL #CustomerBase and #TransactionSummary.

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# 1. Read from Bronze tables
df_customers = spark.table("bronze.Customers")
df_addresses = spark.table("bronze.CustomerAddresses")
df_interactions = spark.table("bronze.CustomerInteractions")
df_orders = spark.table("bronze.Orders")
df_orderdetails = spark.table("bronze.OrderDetails")
df_products = spark.table("bronze.Products")
df_returns = spark.table("bronze.Returns")  # used in next steps

# ----------------------------------------------------------------------------
# Build #CustomerBase logic
# ----------------------------------------------------------------------------
if process_type != "FULL":
    # For incremental logic, filter customers or interactions by data_date
    df_customers = df_customers.filter(
        (F.col("CreatedDate") >= data_date) | (F.col("ModifiedDate") >= data_date)
    )
    df_interactions = df_interactions.filter(F.col("ContactDate") >= data_date)

df_customer_base = (df_customers.alias("c")
    .join(df_addresses.alias("a"),
          (F.col("c.CustomerID") == F.col("a.CustomerID")) &
          (F.col("a.AddressType") == F.lit("Primary")),
          "left")
    .join(df_interactions.alias("i"),
          F.col("c.CustomerID") == F.col("i.CustomerID"),
          "left")
    .groupBy(
        "c.CustomerID", "c.FirstName", "c.LastName",
        "c.Email", "c.Phone",
        "a.StreetAddress", "a.PostalCode", "a.City", "a.State", "a.Country",
        "c.CustomerType", "c.AccountManagerID",
        "c.CreatedDate", "c.ModifiedDate", "c.Status"
    )
    .agg(F.max("i.ContactDate").alias("LastContactDate"))
    .withColumn("CustomerName", F.concat(F.col("FirstName"), F.lit(" "), F.col("LastName")))
    .withColumn("IsActive", F.when(F.col("Status") == "Active", 1).otherwise(0))
    .select(
        F.col("CustomerID"),
        F.col("CustomerName"),
        F.col("Email"), F.col("Phone"),
        F.col("StreetAddress").alias("Address"),
        F.col("PostalCode"), F.col("City"), F.col("State"), F.col("Country"),
        F.col("CustomerType"),
        F.col("AccountManagerID").alias("AccountManager"),
        F.col("CreatedDate"), F.col("ModifiedDate"),
        F.col("IsActive"),
        F.col("LastContactDate")
    )
)

# Write out #CustomerBase as a Silver table
customer_base_silver_path = "/lakehouse/silver/customer_base"
df_customer_base.write.format("delta").mode("overwrite").save(customer_base_silver_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS silver.customer_base "
          f"USING DELTA LOCATION '{customer_base_silver_path}'")

# ----------------------------------------------------------------------------
# Build #TransactionSummary logic
# ----------------------------------------------------------------------------
df_orders_completed = df_orders.filter(F.col("Status") == "Completed")
df_joined_orders = df_orders_completed.alias("o").join(
    df_orderdetails.alias("od"), F.col("o.OrderID") == F.col("od.OrderID"), "inner"
)

# Summarize
df_order_summary = (
    df_joined_orders.groupBy("o.CustomerID")
    .agg(
        F.count_distinct("o.OrderID").alias("TotalOrders"),
        F.sum(F.col("od.Quantity") * F.col("od.UnitPrice") * (1 - F.col("od.Discount"))).alias("TotalSpent"),
        F.min("o.OrderDate").alias("FirstPurchaseDate"),
        F.max("o.OrderDate").alias("LastPurchaseDate")
    )
)

df_transaction_summary = (
    df_order_summary.alias("os")
    .join(df_customer_base.select("CustomerID").alias("cb"), F.col("os.CustomerID") == F.col("cb.CustomerID"), "inner")
    .withColumn("AvgOrderValue", 
                F.when(F.col("TotalOrders") > 0, F.col("TotalSpent") / F.col("TotalOrders"))
                 .otherwise(0))
    .withColumn("DaysSinceLastPurchase",
                F.datediff(F.lit(data_date), F.col("LastPurchaseDate")))
    .withColumn("TopCategory", F.lit(None).cast("string"))
    .withColumn("TopProduct", F.lit(None).cast("string"))
    .withColumn("ReturnRate", F.lit(0).cast("double"))
    .select("os.CustomerID","TotalOrders","TotalSpent","AvgOrderValue",
            "FirstPurchaseDate","LastPurchaseDate","DaysSinceLastPurchase",
            "TopCategory","TopProduct","ReturnRate")
)

# Calculate TopCategory
df_categories = (df_joined_orders
    .join(df_products.alias("p"), F.col("od.ProductID") == F.col("p.ProductID"), "inner")
    .groupBy("o.CustomerID","p.Category")
    .agg(F.sum(F.col("od.Quantity") * F.col("od.UnitPrice") * (1 - F.col("od.Discount"))).alias("CategorySpend"))
)

w_cat = Window.partitionBy("CustomerID").orderBy(F.desc("CategorySpend"))
df_categories_ranked = df_categories.withColumn("CategoryRank", F.row_number().over(w_cat))
df_top_category = df_categories_ranked.filter(F.col("CategoryRank") == 1).select(
    "CustomerID", F.col("Category").alias("TopCategory")
)

df_transaction_summary = (
    df_transaction_summary.alias("ts")
    .join(df_top_category.alias("tc"), "CustomerID", "left")
    .select("ts.*", F.coalesce("tc.TopCategory", "ts.TopCategory").alias("TopCategory"))
)

# Calculate TopProduct
df_products_calc = (df_joined_orders
    .join(df_products.alias("p"), F.col("od.ProductID") == F.col("p.ProductID"), "inner")
    .groupBy("o.CustomerID","p.ProductName")
    .agg(F.sum("od.Quantity").alias("TotalQuantity"))
)

w_prod = Window.partitionBy("CustomerID").orderBy(F.desc("TotalQuantity"))
df_products_ranked = df_products_calc.withColumn("ProductRank", F.row_number().over(w_prod))
df_top_product = df_products_ranked.filter(F.col("ProductRank") == 1).select(
    "CustomerID", F.col("ProductName").alias("TopProduct")
)

df_transaction_summary = (
    df_transaction_summary.alias("ts")
    .join(df_top_product.alias("tp"), "CustomerID", "left")
    .select("ts.*", F.coalesce("tp.TopProduct", "ts.TopProduct").alias("TopProduct"))
)

# Calculate ReturnRate
df_returns_summarized = df_returns.groupBy("CustomerID").agg(F.count_distinct("ReturnID").alias("TotalReturns"))
df_returns_join = df_returns_summarized.alias("r").join(
    df_order_summary.alias("os"), "CustomerID", "inner"
).withColumn(
    "ReturnRate",
    F.when(F.col("os.TotalOrders") > 0, (F.col("r.TotalReturns") / F.col("os.TotalOrders") * 100))
     .otherwise(0)
)

df_transaction_summary = (
    df_transaction_summary.alias("ts")
    .join(df_returns_join.select("CustomerID","ReturnRate"), "CustomerID", "left")
    .select("ts.*", F.coalesce("ReturnRate", "ts.ReturnRate").alias("ReturnRate"))
)

# Write out #TransactionSummary
transaction_summary_silver_path = "/lakehouse/silver/transaction_summary"
df_transaction_summary.write.format("delta").mode("overwrite").save(transaction_summary_silver_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS silver.transaction_summary "
          f"USING DELTA LOCATION '{transaction_summary_silver_path}'")

print("Stage 1 (Base Data) complete: #CustomerBase, #TransactionSummary.")