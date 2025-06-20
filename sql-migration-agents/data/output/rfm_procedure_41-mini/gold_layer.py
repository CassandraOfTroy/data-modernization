# GOLD LAYER START
# ------------------------------------------------------------------------
# Final merges/insertions into production analytics tables:
# "CustomerProfiles" (similar to dbo.CustomerProfiles),
# "CustomerAnalytics" (similar to dbo.CustomerAnalytics),
# "CustomerAnalyticsSummary" (similar to dbo.CustomerAnalyticsSummary).

from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Create or reference existing table paths
gold_profiles_path = "/lakehouse/gold/customer_profiles"
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.customer_profiles USING DELTA LOCATION '{gold_profiles_path}'")

gold_analytics_path = "/lakehouse/gold/customer_analytics"
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.customer_analytics USING DELTA LOCATION '{gold_analytics_path}'")

gold_summary_path = "/lakehouse/gold/customer_analytics_summary"
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.customer_analytics_summary USING DELTA LOCATION '{gold_summary_path}'")

# Prepare data for CustomerProfiles
df_customer_base = spark.table("silver.customer_base")
df_transaction_summary = spark.table("silver.transaction_summary")

df_profiles = (
    df_customer_base.alias("cb")
    .join(df_transaction_summary.alias("ts"), "CustomerID", "left")
    .select(
        "cb.CustomerID","cb.CustomerName","cb.Email","cb.Phone","cb.Address",
        "cb.PostalCode","cb.City","cb.State","cb.Country","cb.CustomerType",
        "cb.AccountManager","cb.CreatedDate","cb.IsActive","cb.LastContactDate",
        "ts.TotalOrders","ts.TotalSpent","ts.AvgOrderValue",
        "ts.FirstPurchaseDate","ts.LastPurchaseDate","ts.DaysSinceLastPurchase",
        "ts.TopCategory","ts.TopProduct","ts.ReturnRate"
    )
)

# MERGE for CustomerProfiles
delta_profiles = DeltaTable.forPath(spark, gold_profiles_path)
delta_profiles.alias("t").merge(
    df_profiles.alias("s"),
    "t.CustomerID = s.CustomerID"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Prepare data for CustomerAnalytics
df_customer_metrics = spark.table("silver.customer_metrics")
df_customer_segments = spark.table("silver.customer_segments")

df_analytics = (
    df_customer_metrics.alias("cm")
    .join(df_customer_segments.alias("cs"), "CustomerID", "inner")
    .select(
        F.lit(data_date).alias("ProcessDate"),
        "cm.CustomerID",
        "cm.CustomerLifetimeValue","cm.RecencyScore",
        "cm.FrequencyScore","cm.MonetaryScore","cm.RFMScore",
        "cm.ChurnProbability","cm.NextPurchasePropensity",
        "cm.LoyaltyIndex","cm.CustomerHealth",
        "cs.ValueSegment","cs.BehaviorSegment","cs.LifecycleSegment",
        "cs.TargetGroup","cs.MarketingRecommendation"
    )
)

# Overwrite logic for the date
delta_analytics = DeltaTable.forPath(spark, gold_analytics_path)
# 1) Delete existing records for the process_date
delta_analytics.delete(condition=f"ProcessDate = '{data_date}'")
# 2) Insert
df_analytics.write.format("delta").mode("append").save(gold_analytics_path)

# Retention cleanup
if not debug_mode:
    # e.g. remove data older than retention_period_days
    cutoff_expr = f"date_sub('{data_date}', {retention_period_days})"
    retention_cutoff = spark.sql(f"SELECT {cutoff_expr} as cutoff").collect()[0]["cutoff"]
    delta_analytics.delete(condition=f"ProcessDate < '{retention_cutoff}'")

# Summaries
processing_start_time = F.current_timestamp()

df_summary = (
    df_customer_base.alias("cb")
    .join(df_customer_metrics.alias("cm"), "CustomerID")
    .join(df_customer_segments.alias("cs"), "CustomerID")
    .select(
       F.lit(data_date).alias("ProcessDate"),
       F.lit(process_type).alias("ProcessType"),
       F.count_distinct("cb.CustomerID").over(Window.partitionBy()).alias("TotalCustomers"),
       F.sum(F.when(F.col("cs.LifecycleSegment") == "Active", 1).otherwise(0)).over(Window.partitionBy()).alias("ActiveCustomers"),
       F.sum(F.when(F.col("cs.LifecycleSegment") == "New Customer", 1).otherwise(0)).over(Window.partitionBy()).alias("NewCustomers"),
       F.sum(F.when((F.col("cs.LifecycleSegment") == "At Risk") | (F.col("cs.LifecycleSegment") == "Churned"), 1).otherwise(0)).over(Window.partitionBy()).alias("ChurningCustomers"),
       F.sum(F.when(F.col("cs.ValueSegment") == "High Value", 1).otherwise(0)).over(Window.partitionBy()).alias("HighValueCustomers"),
       F.avg("cm.CustomerLifetimeValue").over(Window.partitionBy()).alias("AverageLifetimeValue")
    ).distinct()
)

df_summary_final = df_summary.withColumn(
    "TotalProcessingTimeMs",
    (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(processing_start_time)) * 1000
).withColumn("ProcessingStatus", F.lit("Success"))

df_summary_final.write.format("delta").mode("append").save(gold_summary_path)

print("Gold layer complete: CustomerProfiles, CustomerAnalytics, CustomerAnalyticsSummary.")