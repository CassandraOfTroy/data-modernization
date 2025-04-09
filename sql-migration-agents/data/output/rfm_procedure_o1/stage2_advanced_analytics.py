# STAGE 2: ADVANCED ANALYTICS START
# -------------------------------------------------------------
# This stage computes advanced analytics: RFM, churn probability,
# next purchase propensity, loyalty index, and segmentation.
# Equivalent to #CustomerMetrics and #CustomerSegments.

# 3a) Identify top category & product, plus return rates, finishing #TransactionSummary logic
from pyspark.sql.window import Window

# Top category
categorySpendDF = (
    ordersBronzeDF.alias("o")
    .join(orderDetailsBronzeDF.alias("od"), "OrderID")
    .join(productsBronzeDF.alias("p"), "ProductID")
    .where(F.col("o.Status") == "Completed")
    .groupBy("o.CustomerID", "p.Category")
    .agg(F.sum(F.col("od.Quantity") * F.col("od.UnitPrice") * (1 - F.col("od.Discount"))).alias("CategorySpend"))
)

catWindow = Window.partitionBy("CustomerID").orderBy(F.col("CategorySpend").desc())
catTopDF = categorySpendDF.withColumn("CategoryRank", F.row_number().over(catWindow)).filter(F.col("CategoryRank") == 1)

# Top product
productQtyDF = (
    ordersBronzeDF.alias("o")
    .join(orderDetailsBronzeDF.alias("od"), "OrderID")
    .join(productsBronzeDF.alias("p"), "ProductID")
    .where(F.col("o.Status") == "Completed")
    .groupBy("o.CustomerID", "p.ProductName")
    .agg(F.sum("od.Quantity").alias("TotalQuantity"))
)

prodWindow = Window.partitionBy("CustomerID").orderBy(F.col("TotalQuantity").desc())
prodTopDF = productQtyDF.withColumn("ProductRank", F.row_number().over(prodWindow)).filter(F.col("ProductRank") == 1)

# Return rate
returnsGroupedDF = (
    returnsBronzeDF.groupBy("CustomerID")
    .agg(F.countDistinct("ReturnID").alias("TotalReturns"))
)

# Complete the transactionSummaryDF with top category, product, and return rate
transactionSummaryDF = (
    transactionSummaryDF.alias("ts")
    .join(catTopDF.alias("cat"), "CustomerID", "left")
    .join(prodTopDF.alias("prod"), "CustomerID", "left")
    .join(returnsGroupedDF.alias("ret"), "CustomerID", "left")
    .select(
        F.col("ts.CustomerID"),
        F.col("ts.TotalOrders"),
        F.col("ts.TotalSpent"),
        F.col("ts.AvgOrderValue"),
        F.col("ts.FirstPurchaseDate"),
        F.col("ts.LastPurchaseDate"),
        F.col("ts.DaysSinceLastPurchase"),
        F.col("cat.Category").alias("TopCategory"),
        F.col("prod.ProductName").alias("TopProduct"),
        F.when(F.col("ts.TotalOrders") > 0, (F.col("ret.TotalReturns") / F.col("ts.TotalOrders")) * 100)
         .otherwise(F.lit(0.0)).alias("ReturnRate")
    )
)

# 3b) Calculate RFM (Recency, Frequency, Monetary) scores
windowRecency = Window.orderBy(F.col("DaysSinceLastPurchase").asc())
windowFrequency = Window.orderBy(F.col("TotalOrders").desc())
windowMonetary = Window.orderBy(F.col("TotalSpent").desc())

transactionSummaryDF = (
    transactionSummaryDF
    .withColumn("RecencyScore", F.ntile(5).over(windowRecency))
    .withColumn("FrequencyScore", F.ntile(5).over(windowFrequency))
    .withColumn("MonetaryScore", F.ntile(5).over(windowMonetary))
)

rfmDF = transactionSummaryDF.select(
    "CustomerID",
    "RecencyScore",
    "FrequencyScore",
    "MonetaryScore",
    (F.col("RecencyScore") + F.col("FrequencyScore") + F.col("MonetaryScore")).alias("RFMScore")
)

# 3c) Calculate churn probability, next purchase propensity, loyalty
churnDF = (
    transactionSummaryDF.select(
        "CustomerID",
        F.when(F.col("DaysSinceLastPurchase") > churnDaysVeryHigh, 0.8)
         .when(F.col("DaysSinceLastPurchase") > churnDaysHigh, 0.5)
         .when(F.col("DaysSinceLastPurchase") > churnDaysMedium, 0.3)
         .when(F.col("DaysSinceLastPurchase") > churnDaysLow, 0.1)
         .otherwise(0.05).alias("ChurnProbability"),
        F.when((F.col("DaysSinceLastPurchase") < churnDaysLow) & (F.col("TotalOrders") > 3), 0.8)
         .when((F.col("DaysSinceLastPurchase") < churnDaysMedium) & (F.col("TotalOrders") > 2), 0.6)
         .when(F.col("DaysSinceLastPurchase") < churnDaysHigh, 0.4)
         .otherwise(0.2).alias("NextPurchasePropensity"),
        (
            F.when(
                (F.col("TotalOrders") >= loyaltyThreshold) & (F.col("ReturnRate") < 10),
                (F.col("TotalOrders") * 0.6) + ((100 - F.col("ReturnRate")) * 0.4)
            )
            .otherwise(F.col("TotalOrders") * 0.4)
            / 10
        ).alias("LoyaltyIndex")
    )
)

customerMetricsDF = (
    rfmDF.alias("r")
    .join(transactionSummaryDF.alias("t"), "CustomerID")
    .join(churnDF.alias("ch"), "CustomerID")
    .select(
        F.col("r.CustomerID"),
        (F.col("t.TotalSpent") * (1 + (1 - F.col("ch.ChurnProbability")))).alias("CustomerLifetimeValue"),
        "r.RecencyScore",
        "r.FrequencyScore",
        "r.MonetaryScore",
        "r.RFMScore",
        "ch.ChurnProbability",
        "ch.NextPurchasePropensity",
        "ch.LoyaltyIndex",
        F.when(F.col("ch.ChurnProbability") > 0.7, "At Risk")
         .when(F.col("ch.LoyaltyIndex") > 0.8, "Excellent")
         .when(F.col("ch.LoyaltyIndex") > 0.6, "Good")
         .when(F.col("ch.LoyaltyIndex") > 0.4, "Average")
         .otherwise("Needs Attention").alias("CustomerHealth")
    )
)

# 3d) Create customer segmentation
segmentationDF = (
    customerBaseDF.alias("cb")
    .join(customerMetricsDF.alias("cm"), "CustomerID")
    .join(transactionSummaryDF.alias("ts"), "CustomerID")
    .select(
        F.col("cb.CustomerID"),
        # ValueSegment
        F.when(F.col("cm.CustomerLifetimeValue") >= highValueThreshold, "High Value")
         .when(F.col("cm.CustomerLifetimeValue") >= highValueThreshold * 0.5, "Medium Value")
         .otherwise("Low Value").alias("ValueSegment"),
        # BehaviorSegment
        F.when(F.col("cm.RFMScore") >= 13, "Champions")
         .when((F.col("cm.RecencyScore") >= 4) & (F.col("cm.FrequencyScore") >= 3), "Loyal Customers")
         .when((F.col("cm.RecencyScore") >= 4) & (F.col("cm.FrequencyScore") <= 2), "Potential Loyalists")
         .when((F.col("cm.RecencyScore") <= 2) & (F.col("cm.FrequencyScore") >= 3) & (F.col("cm.MonetaryScore") >= 3), "At Risk")
         .when((F.col("cm.RecencyScore") <= 2) & (F.col("cm.FrequencyScore") <= 2) & (F.col("cm.MonetaryScore") <= 2), "Hibernating")
         .when(F.col("cm.RecencyScore") <= 1, "Lost")
         .otherwise("Others").alias("BehaviorSegment"),
        # LifecycleSegment
        F.when((F.col("ts.TotalOrders") == 1) & (F.col("ts.DaysSinceLastPurchase") <= 30), "New Customer")
         .when((F.col("ts.TotalOrders") > 1) & (F.col("cm.ChurnProbability") < 0.3), "Active")
         .when((F.col("cm.ChurnProbability") >= 0.3) & (F.col("cm.ChurnProbability") < 0.7), "At Risk")
         .when(F.col("cm.ChurnProbability") >= 0.7, "Churned")
         .otherwise("Inactive").alias("LifecycleSegment"),
        # TargetGroup
        F.when((F.col("cm.CustomerLifetimeValue") >= highValueThreshold) & (F.col("cm.ChurnProbability") < 0.3), "VIP")
         .when((F.col("cm.CustomerLifetimeValue") >= highValueThreshold * 0.5) & (F.col("cm.ChurnProbability") >= 0.3), "Retention Priority")
         .when((F.col("ts.TotalOrders") == 1) & (F.col("ts.DaysSinceLastPurchase") <= 30), "Nurture New")
         .when(F.col("cm.NextPurchasePropensity") > 0.6, "Growth Opportunity")
         .when(F.col("ts.ReturnRate") > 20, "Service Improvement")
         .otherwise("Standard").alias("TargetGroup"),
        # MarketingRecommendation
        F.when(
            (F.col("cm.CustomerLifetimeValue") >= highValueThreshold) & (F.col("cm.ChurnProbability") < 0.3),
            "Exclusive offers, VIP events, Personal shopping assistance, Early access to new products"
        ).when(
            (F.col("cm.CustomerLifetimeValue") >= highValueThreshold * 0.5) & (F.col("cm.ChurnProbability") >= 0.3),
            "Re-engagement campaign, Loyalty rewards, Personalized recommendations based on past purchases"
        ).when(
            (F.col("ts.TotalOrders") == 1) & (F.col("ts.DaysSinceLastPurchase") <= 30),
            "Welcome series, Educational content, First purchase follow-up, Category exploration"
        ).when(
            F.col("cm.NextPurchasePropensity") > 0.6,
            F.concat(
                F.lit("Cross-sell/upsell, Bundle offers based on "),
                F.coalesce(F.col("ts.TopCategory"), F.lit("preferred category")),
                F.lit(", Category expansion")
            )
        ).when(
            F.col("ts.ReturnRate") > 20,
            "Satisfaction survey, Improved product information, Size/fit guides, Service recovery"
        ).otherwise(
            F.concat(
                F.lit("Standard seasonal promotions, Category newsletters, Reactivation after "),
                (F.col("ts.DaysSinceLastPurchase") / 30).cast("int"),
                F.lit(" months")
            )
        ).alias("MarketingRecommendation")
    )
)

# Optionally, write out these silver DataFrames (RFM, Metrics, Segments)
customerBaseDF.write.format("delta").mode("overwrite").saveAsTable("lakehouse_silver.CustomerBase")
transactionSummaryDF.write.format("delta").mode("overwrite").saveAsTable("lakehouse_silver.TransactionSummary")
customerMetricsDF.write.format("delta").mode("overwrite").saveAsTable("lakehouse_silver.CustomerMetrics")
segmentationDF.write.format("delta").mode("overwrite").saveAsTable("lakehouse_silver.CustomerSegments")

# STAGE 2: ADVANCED ANALYTICS END