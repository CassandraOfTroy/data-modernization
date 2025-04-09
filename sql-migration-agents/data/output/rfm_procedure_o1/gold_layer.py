# GOLD LAYER START
# -------------------------------------------------------------
# Produce final analytics tables (CustomerProfiles, CustomerAnalytics)
# and daily summary (CustomerAnalyticsSummary). We can use merges or
# simple overwrites/appends for demonstration.

# 1) Update or overwrite the CustomerProfiles table
customerProfileGoldDF = (
    customerBaseDF.alias("cb")
    .join(transactionSummaryDF.alias("ts"), "CustomerID", "left")
    .select(
        "cb.CustomerID",
        "cb.CustomerName",
        "cb.Email",
        "cb.Phone",
        "cb.Address",
        "cb.PostalCode",
        "cb.City",
        "cb.State",
        "cb.Country",
        "cb.CustomerType",
        "cb.AccountManager",
        "cb.CreatedDate",
        "cb.IsActive",
        "cb.LastContactDate",
        "ts.TotalOrders",
        "ts.TotalSpent",
        "ts.AvgOrderValue",
        "ts.FirstPurchaseDate",
        "ts.LastPurchaseDate",
        "ts.DaysSinceLastPurchase",
        "ts.TopCategory",
        "ts.TopProduct",
        "ts.ReturnRate",
        F.current_timestamp().alias("ModifiedDate")  # for auditing
    )
)

(
    customerProfileGoldDF
    .write
    .format("delta")
    .mode("overwrite")  # For incremental, consider MERGE. 
    .saveAsTable("lakehouse_gold.CustomerProfiles")
)

# 2) Insert daily snapshot into CustomerAnalytics
customerAnalyticsGoldDF = (
    customerMetricsDF.alias("cm")
    .join(segmentationDF.alias("cs"), "CustomerID")
    .select(
        F.lit(dataDate).alias("ProcessDate"),
        "cm.CustomerID",
        "cm.CustomerLifetimeValue",
        "cm.RecencyScore",
        "cm.FrequencyScore",
        "cm.MonetaryScore",
        "cm.RFMScore",
        "cm.ChurnProbability",
        "cm.NextPurchasePropensity",
        "cm.LoyaltyIndex",
        "cm.CustomerHealth",
        "cs.ValueSegment",
        "cs.BehaviorSegment",
        "cs.LifecycleSegment",
        "cs.TargetGroup",
        "cs.MarketingRecommendation"
    )
)

(
    customerAnalyticsGoldDF
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("lakehouse_gold.CustomerAnalytics")
)

# Retention: remove older data
spark.sql(f"""
    DELETE FROM lakehouse_gold.CustomerAnalytics
    WHERE ProcessDate < DATEADD(DAY, -{retentionPeriodDays}, '{str(dataDate)}')
""")

# 3) Generate daily summary (CustomerAnalyticsSummary)
summaryWindow = Window.partitionBy()
summaryDF = (
    customerBaseDF.alias("cb")
    .join(customerMetricsDF.alias("cm"), "CustomerID")
    .join(segmentationDF.alias("cs"), "CustomerID")
    .select(
        F.lit(dataDate).alias("ProcessDate"),
        F.lit(processType).alias("ProcessType"),
        F.count_distinct("cb.CustomerID").over(summaryWindow).alias("TotalCustomers"),
        F.sum(F.when(F.col("cs.LifecycleSegment") == "Active", 1).otherwise(0)).over(summaryWindow).alias("ActiveCustomers"),
        F.sum(F.when(F.col("cs.LifecycleSegment") == "New Customer", 1).otherwise(0)).over(summaryWindow).alias("NewCustomers"),
        F.sum(F.when((F.col("cs.LifecycleSegment") == "At Risk") | (F.col("cs.LifecycleSegment") == "Churned"), 1).otherwise(0)).over(summaryWindow).alias("ChurningCustomers"),
        F.sum(F.when(F.col("cs.ValueSegment") == "High Value", 1).otherwise(0)).over(summaryWindow).alias("HighValueCustomers"),
        F.avg("cm.CustomerLifetimeValue").over(summaryWindow).alias("AverageLifetimeValue"),
        (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.lit(str(dataDate)))) * 1000,
        F.lit("Success").alias("ProcessingStatus")
    )
).distinct()

(
    summaryDF
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("lakehouse_gold.CustomerAnalyticsSummary")
)

if debugMode:
    summaryDF.show()

# GOLD LAYER END