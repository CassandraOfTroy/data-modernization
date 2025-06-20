# STAGE 2: ADVANCED ANALYTICS START
# ------------------------------------------------------------------------
# Creates intermediate DataFrames equivalent to #CustomerMetrics (RFM, churn)
# and #CustomerSegments (segmentation logic).

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Read Stage 1 results
df_customer_base = spark.table("silver.customer_base")
df_transaction_summary = spark.table("silver.transaction_summary")

# ----------------------------------------------------------------------------
# #CustomerMetrics (RFM, churn, loyalty, etc.)
# ----------------------------------------------------------------------------
df_ts = df_transaction_summary.alias("ts")

# RFM scoring (NTILE=5)
w_recency_asc = Window.orderBy(F.coalesce(F.col("DaysSinceLastPurchase"), F.lit(999999)).asc())
df_ts = df_ts.withColumn("RecencyScore", F.ntile(5).over(w_recency_asc))

w_freq_desc = Window.orderBy(F.coalesce(F.col("TotalOrders"), F.lit(0)).desc())
df_ts = df_ts.withColumn("FrequencyScore", F.ntile(5).over(w_freq_desc))

w_monetary_desc = Window.orderBy(F.coalesce(F.col("TotalSpent"), F.lit(0)).desc())
df_ts = df_ts.withColumn("MonetaryScore", F.ntile(5).over(w_monetary_desc))

df_rfm_scored = df_ts.select(
    "CustomerID",
    "TotalOrders",
    "TotalSpent",
    "DaysSinceLastPurchase",
    "RecencyScore",
    "FrequencyScore",
    "MonetaryScore",
    (F.col("RecencyScore") + F.col("FrequencyScore") + F.col("MonetaryScore")).alias("RFMScore")
)

# Calculate churn probability, next purchase propensity, loyalty index
df_customer_metrics = (df_rfm_scored
    .withColumn(
        "ChurnProbability",
        F.when(F.col("DaysSinceLastPurchase") > churn_days_very_high, 0.8)
         .when(F.col("DaysSinceLastPurchase") > churn_days_high, 0.5)
         .when(F.col("DaysSinceLastPurchase") > churn_days_medium, 0.3)
         .when(F.col("DaysSinceLastPurchase") > churn_days_low, 0.1)
         .otherwise(0.05)
    )
    .withColumn(
        "NextPurchasePropensity",
        F.when((F.col("DaysSinceLastPurchase") < churn_days_low) & (F.col("TotalOrders") > 3), 0.8)
         .when((F.col("DaysSinceLastPurchase") < churn_days_medium) & (F.col("TotalOrders") > 2), 0.6)
         .when(F.col("DaysSinceLastPurchase") < churn_days_high, 0.4)
         .otherwise(0.2)
    )
    # Simplified loyalty index
    .withColumn(
        "LoyaltyIndex",
        F.when(F.col("TotalOrders") >= loyalty_threshold,
               ((F.col("TotalOrders") * 0.6) + (100.0 * 0.4)) / 10
              ).otherwise((F.col("TotalOrders") * 0.4) / 10)
    )
    .withColumn(
        "CustomerLifetimeValue",
        F.col("TotalSpent") * (1 + (1 - F.col("ChurnProbability")))
    )
    .withColumn(
        "CustomerHealth",
        F.when(F.col("ChurnProbability") > 0.7, "At Risk")
         .when(F.col("LoyaltyIndex") > 0.8, "Excellent")
         .when(F.col("LoyaltyIndex") > 0.6, "Good")
         .when(F.col("LoyaltyIndex") > 0.4, "Average")
         .otherwise("Needs Attention")
    )
    .select(
        "CustomerID",
        "CustomerLifetimeValue",
        "RecencyScore",
        "FrequencyScore",
        "MonetaryScore",
        "RFMScore",
        "ChurnProbability",
        "NextPurchasePropensity",
        "LoyaltyIndex",
        "CustomerHealth"
    )
)

customer_metrics_silver_path = "/lakehouse/silver/customer_metrics"
df_customer_metrics.write.format("delta").mode("overwrite").save(customer_metrics_silver_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS silver.customer_metrics "
          f"USING DELTA LOCATION '{customer_metrics_silver_path}'")

# ----------------------------------------------------------------------------
# #CustomerSegments
# ----------------------------------------------------------------------------
df_cm = spark.table("silver.customer_metrics").alias("cm")
df_cs = (df_customer_base.alias("cb")
         .join(df_cm, "CustomerID", "inner")
         .join(df_transaction_summary.alias("ts"), "CustomerID", "inner")
         .select(
             F.col("cb.CustomerID"),

             # Value Segment
             F.when(F.col("cm.CustomerLifetimeValue") >= high_value_threshold, "High Value")
              .when(F.col("cm.CustomerLifetimeValue") >= (high_value_threshold * 0.5), "Medium Value")
              .otherwise("Low Value").alias("ValueSegment"),

             # Behavior Segment
             F.when(F.col("cm.RFMScore") >= 13, "Champions")
              .when((F.col("cm.RecencyScore") >= 4) & (F.col("cm.FrequencyScore") >= 3), "Loyal Customers")
              .when((F.col("cm.RecencyScore") >= 4) & (F.col("cm.FrequencyScore") <= 2), "Potential Loyalists")
              .when((F.col("cm.RecencyScore") <= 2) & (F.col("cm.FrequencyScore") >= 3) & (F.col("cm.MonetaryScore") >= 3), "At Risk")
              .when((F.col("cm.RecencyScore") <= 2) & (F.col("cm.FrequencyScore") <= 2) & (F.col("cm.MonetaryScore") <= 2), "Hibernating")
              .when(F.col("cm.RecencyScore") <= 1, "Lost")
              .otherwise("Others")
              .alias("BehaviorSegment"),

             # Lifecycle Segment
             F.when((F.col("ts.TotalOrders") == 1) & (F.col("ts.DaysSinceLastPurchase") <= 30), "New Customer")
              .when((F.col("ts.TotalOrders") > 1) & (F.col("cm.ChurnProbability") < 0.3), "Active")
              .when((F.col("cm.ChurnProbability") >= 0.3) & (F.col("cm.ChurnProbability") < 0.7), "At Risk")
              .when(F.col("cm.ChurnProbability") >= 0.7, "Churned")
              .otherwise("Inactive")
              .alias("LifecycleSegment"),

             # TargetGroup
             F.when((F.col("cm.CustomerLifetimeValue") >= high_value_threshold) & (F.col("cm.ChurnProbability") < 0.3), "VIP")
              .when((F.col("cm.CustomerLifetimeValue") >= (high_value_threshold * 0.5)) & (F.col("cm.ChurnProbability") >= 0.3), "Retention Priority")
              .when((F.col("ts.TotalOrders") == 1) & (F.col("ts.DaysSinceLastPurchase") <= 30), "Nurture New")
              .when(F.col("cm.NextPurchasePropensity") > 0.6, "Growth Opportunity")
              .when(F.col("ts.ReturnRate") > 20, "Service Improvement")
              .otherwise("Standard")
              .alias("TargetGroup"),

             # MarketingRecommendation
             F.when(
                 (F.col("cm.CustomerLifetimeValue") >= high_value_threshold) & (F.col("cm.ChurnProbability") < 0.3),
                 "Exclusive offers, VIP events, Personal shopping assistance, Early access to new products"
              )
              .when(
                 (F.col("cm.CustomerLifetimeValue") >= (high_value_threshold * 0.5)) & (F.col("cm.ChurnProbability") >= 0.3),
                 "Re-engagement campaign, Loyalty rewards, Personalized recommendations based on past purchases"
              )
              .when(
                 (F.col("ts.TotalOrders") == 1) & (F.col("ts.DaysSinceLastPurchase") <= 30),
                 "Welcome series, Educational content, First purchase follow-up, Category exploration"
              )
              .when(
                 F.col("cm.NextPurchasePropensity") > 0.6,
                 F.concat(
                     F.lit("Cross-sell/upsell, Bundle offers based on "),
                     F.coalesce(F.col("ts.TopCategory"), F.lit("preferred category")),
                     F.lit(", Category expansion")
                 )
              )
              .when(
                 F.col("ts.ReturnRate") > 20,
                 "Satisfaction survey, Improved product information, Size/fit guides, Service recovery"
              )
              .otherwise(
                 F.concat(
                     F.lit("Standard seasonal promotions, Category newsletters, Reactivation after "),
                     (F.coalesce(F.col("ts.DaysSinceLastPurchase"), F.lit(0)) / F.lit(30)).cast("int"),
                     F.lit(" months")
                 )
              ).alias("MarketingRecommendation")
         )
)

customer_segments_silver_path = "/lakehouse/silver/customer_segments"
df_cs.write.format("delta").mode("overwrite").save(customer_segments_silver_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS silver.customer_segments "
          f"USING DELTA LOCATION '{customer_segments_silver_path}'")

print("Stage 2 (Advanced Analytics) complete: #CustomerMetrics, #CustomerSegments.")