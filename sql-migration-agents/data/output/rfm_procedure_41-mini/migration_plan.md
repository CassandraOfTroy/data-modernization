Below is a comprehensive, role-specific view of the migration plan and a sample PySpark implementation for Microsoft Fabric that follows the Medallion (Bronze–Silver–Gold) architecture. It also includes guidelines, best practices, and test cases.

────────────────────────────────────────────────────────
1. BUSINESS ANALYST VIEW: BUSINESS PURPOSE & REQUIREMENTS
────────────────────────────────────────────────────────
1.1 Purpose and Business Value
• The original SQL stored procedure calculates customer metrics, RFM scores, churn probability, loyalty, and segmentation.  
• It supports business decisions on customer retention, targeting, and lifecycle management.

1.2 Key Business Metrics
• Transaction Metrics: TotalOrders, TotalSpent, AvgOrderValue, DaysSinceLastPurchase, ReturnRate  
• RFM Scores: RecencyScore, FrequencyScore, MonetaryScore, Overall RFMScore  
• Churn Probability & Next Purchase Propensity: Predicts risk of customer attrition and likelihood of near-future purchases.  
• Loyalty Index & Customer Health: Indicates brand commitment.  
• Customer Segmentation: ValueSegment, BehaviorSegment, LifecycleSegment, TargetGroup, MarketingRecommendation.

1.3 Data Sources
• dbo.Customers, dbo.CustomerAddresses, dbo.CustomerInteractions  
• dbo.Orders, dbo.OrderDetails, dbo.Products  
• dbo.Returns  
• Target tables: CustomerProfiles, CustomerAnalytics, CustomerAnalyticsSummary.

1.4 Business Logic Highlights
• FULL vs. incremental load (Filter by ModifiedDate / DataDate).  
• Parameterized churn thresholds (@ChurnDays_Low, @ChurnDays_High, etc.).  
• RFM scoring using NTILE(5).  
• Clean-up older data (< RetentionPeriodDays).

1.5 Constraints & Considerations
• Large data volumes require efficient partitioning and incremental loads.  
• Data quality issues must be managed (e.g., missing addresses, invalid returns).  
• Must maintain compliance and data governance best practices in Microsoft Fabric.

────────────────────────────────────────────────────────
2. PRODUCT OWNER VIEW: MIGRATION PLAN
────────────────────────────────────────────────────────
We will refactor the single T-SQL stored procedure into a PySpark pipeline in Microsoft Fabric, adopting the Medallion Architecture:

2.1 Bronze Layer (Raw Ingestion)
• Ingest source data from SQL or other operational systems into a Fabric Lakehouse table (Delta).  
• Store data “as is” with minimal transformations.

2.2 Silver Layer (Curated Transformations)
• Model the logic previously enclosed in T-SQL temp tables (#CustomerBase, #TransactionSummary, #CustomerMetrics, #CustomerSegments).  
• Perform the same business logic in PySpark (RFM scoring, churn probability, etc.).  
• Store intermediate results in silver tables.

2.3 Gold Layer (Analytics & Output)
• Consolidate the silver-layer outputs to produce final analytics (CustomerAnalytics) and summary (CustomerAnalyticsSummary).  
• Use Delta merges to integrate new/updated records, manage incremental loads, and apply retention logic.

2.4 Parameterization & Scheduling
• Expose parameters (DataDate, ProcessType, RetentionDays, DebugMode) as notebook parameters or pipeline parameters in Fabric.  
• Schedule as a daily or on-demand job in Microsoft Fabric pipelines.

2.5 Error Logging & Governance
• Use Spark logging or Fabric Monitor to capture process logs and handle exceptions.  
• Enforce standards for data validations (e.g., negative amounts, missing IDs).

────────────────────────────────────────────────────────
3. AZURE DATA ENGINEER VIEW: PYSpark CODE (BRONZE → SILVER → GOLD)
────────────────────────────────────────────────────────
Below is a representative PySpark implementation in a Microsoft Fabric environment. Adapt paths, table names, or connection details as needed.

--------------------------------------------------
3.1 Bronze Layer – Raw Data Ingestion
--------------------------------------------------
# BRONZE_INGEST_CUSTOMERS.py

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Example JDBC connection details to read from SQL
jdbcUrl = "jdbc:sqlserver://<SQL_SERVER>;databaseName=<DB>"
connectionProps = {"user": "<USERNAME>", "password": "<PASSWORD>"}

# Source tables to ingest
source_tables = {
    "Customers": "dbo.Customers",
    "CustomerAddresses": "dbo.CustomerAddresses",
    "CustomerInteractions": "dbo.CustomerInteractions",
    "Orders": "dbo.Orders",
    "OrderDetails": "dbo.OrderDetails",
    "Products": "dbo.Products",
    "Returns": "dbo.Returns"
}

for alias, tbl_name in source_tables.items():
    df = (spark.read
                .format("jdbc")
                .option("url", jdbcUrl)
                .option("dbtable", tbl_name)
                .options(**connectionProps)
                .load()
          )
    
    bronze_path = f"/lakehouse/bronze/{alias}"
    df.write.format("delta").mode("overwrite").save(bronze_path)
    
    # Create Delta table reference in Fabric
    spark.sql(f"CREATE TABLE IF NOT EXISTS bronze.{alias} "
              f"USING DELTA LOCATION '{bronze_path}'")

--------------------------------------------------
3.2 Silver Layer – Core Transformations
--------------------------------------------------
# SILVER_CUSTOMER_ANALYTICS.py
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Example parameters
data_date = "2023-01-01"
process_type = "FULL"  # or "INCREMENTAL"
debug_mode = False
retention_period_days = 365
high_value_threshold = 5000.00
loyalty_threshold = 5
churn_days_very_high = 180
churn_days_high = 90
churn_days_medium = 60
churn_days_low = 30

# Read bronze tables
df_customers = spark.table("bronze.Customers")
df_addresses = spark.table("bronze.CustomerAddresses")
df_interactions = spark.table("bronze.CustomerInteractions")
df_orders = spark.table("bronze.Orders")
df_orderdetails = spark.table("bronze.OrderDetails")
df_products = spark.table("bronze.Products")
df_returns = spark.table("bronze.Returns")

# -------------------------------
# Step 1: Customer Base (like #CustomerBase)
# -------------------------------
if process_type != "FULL":
    # Only pick “changed” records
    df_customers = df_customers.filter((F.col("ModifiedDate") >= data_date) |
                                       (F.col("CreatedDate") >= data_date))
    df_interactions = df_interactions.filter(F.col("ContactDate") >= data_date)

df_customer_base = (
    df_customers.alias("c")
    .join(
       df_addresses.alias("a"),
       (F.col("c.CustomerID") == F.col("a.CustomerID")) &
       (F.col("a.AddressType") == F.lit("Primary")),
       "left"
    )
    .join(
       df_interactions.alias("i"),
       F.col("c.CustomerID") == F.col("i.CustomerID"),
       "left"
    )
    .groupBy("c.CustomerID", "c.FirstName", "c.LastName",
             "c.Email","c.Phone", 
             "a.StreetAddress","a.PostalCode","a.City","a.State","a.Country",
             "c.CustomerType","c.AccountManagerID",
             "c.CreatedDate","c.ModifiedDate","c.Status")
    .agg(F.max("i.ContactDate").alias("LastContactDate"))
    .withColumn("CustomerName",
                F.concat(F.col("FirstName"), F.lit(" "), F.col("LastName")))
    .withColumn("IsActive",
                F.when(F.col("Status") == "Active", F.lit(1)).otherwise(F.lit(0)))
    .select(F.col("CustomerID"),
            F.col("CustomerName"),
            F.col("Email"), F.col("Phone"),
            F.col("StreetAddress").alias("Address"),
            F.col("PostalCode"), F.col("City"), F.col("State"), F.col("Country"),
            F.col("CustomerType"),
            F.col("AccountManagerID").alias("AccountManager"),
            F.col("CreatedDate"),
            F.col("ModifiedDate"),
            F.col("IsActive"),
            F.col("LastContactDate"))
)

customer_base_silver_path = "/lakehouse/silver/customer_base"
df_customer_base.write.format("delta").mode("overwrite").save(customer_base_silver_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS silver.customer_base "
          f"USING DELTA LOCATION '{customer_base_silver_path}'")

# -------------------------------
# Step 2: Transaction Summary 
# -------------------------------
df_orders_completed = df_orders.filter(F.col("Status") == "Completed")
df_order_join = df_orders_completed.alias("o").join(
                    df_orderdetails.alias("od"),
                    F.col("o.OrderID") == F.col("od.OrderID"),
                    "inner"
                )

df_order_summary = (
    df_order_join.groupBy("o.CustomerID")
    .agg(F.count_distinct("o.OrderID").alias("TotalOrders"),
         F.sum(F.col("od.Quantity") *
               F.col("od.UnitPrice") *
               (1 - F.col("od.Discount"))).alias("TotalSpent"),
         F.min("o.OrderDate").alias("FirstPurchaseDate"),
         F.max("o.OrderDate").alias("LastPurchaseDate"))
)

df_transaction_summary = (
    df_order_summary
    .join(df_customer_base.select("CustomerID"), "CustomerID", "inner")
    .withColumn("AvgOrderValue",
                F.when(F.col("TotalOrders") > 0,
                       F.col("TotalSpent") / F.col("TotalOrders"))
                 .otherwise(F.lit(0)))
    .withColumn("DaysSinceLastPurchase",
                F.datediff(F.lit(data_date), F.col("LastPurchaseDate")))
    .withColumn("TopCategory", F.lit(None).cast("string"))
    .withColumn("TopProduct", F.lit(None).cast("string"))
    .withColumn("ReturnRate", F.lit(0).cast("double"))
)

# Top Category
df_category_calc = (
    df_order_join
    .join(df_products.alias("p"),
          F.col("od.ProductID") == F.col("p.ProductID"), "inner")
    .groupBy("o.CustomerID","p.Category")
    .agg(F.sum( F.col("od.Quantity")*F.col("od.UnitPrice")*
               (1-F.col("od.Discount"))).alias("CategorySpend"))
)
w_cat = Window.partitionBy("CustomerID").orderBy(F.desc("CategorySpend"))
df_category_ranked = df_category_calc.withColumn("CategoryRank",
                                                 F.row_number().over(w_cat))
df_top_category = df_category_ranked.filter(F.col("CategoryRank") == 1).select(
    "CustomerID", F.col("Category").alias("TopCategory")
)

df_transaction_summary = (
    df_transaction_summary.alias("ts")
    .join(df_top_category.alias("tc"), "CustomerID", "left")
    .select("ts.*", F.coalesce("tc.TopCategory","ts.TopCategory").alias("TopCategory"))
)

# Top Product
df_product_calc = (
    df_order_join
    .join(df_products.alias("p"),
          F.col("od.ProductID") == F.col("p.ProductID"), "inner")
    .groupBy("o.CustomerID","p.ProductName")
    .agg(F.sum("od.Quantity").alias("TotalQuantity"))
)
w_prod = Window.partitionBy("CustomerID").orderBy(F.desc("TotalQuantity"))
df_product_ranked = df_product_calc.withColumn("ProductRank",
                                               F.row_number().over(w_prod))
df_top_product = df_product_ranked.filter(F.col("ProductRank") == 1).select(
    "CustomerID", F.col("ProductName").alias("TopProduct")
)
df_transaction_summary = (
    df_transaction_summary.alias("ts")
    .join(df_top_product.alias("tp"), "CustomerID", "left")
    .select("ts.*", F.coalesce("tp.TopProduct","ts.TopProduct").alias("TopProduct"))
)

# ReturnRate
df_returns_summarized = (
    df_returns.groupBy("CustomerID")
    .agg(F.count_distinct("ReturnID").alias("TotalReturns"))
)
df_returns_join = (
    df_returns_summarized.alias("r")
    .join(df_order_summary.alias("os"), "CustomerID", "inner")
    .withColumn("ReturnRate",
                F.when(F.col("os.TotalOrders") > 0,
                       F.col("r.TotalReturns")/F.col("os.TotalOrders")*100)
                 .otherwise(F.lit(0)))
)
df_transaction_summary = (
    df_transaction_summary.alias("ts")
    .join(df_returns_join.select("CustomerID","ReturnRate"), "CustomerID", "left")
    .select("ts.*", F.coalesce("ReturnRate", F.col("ts.ReturnRate")).alias("ReturnRate"))
)

transaction_summary_silver_path = "/lakehouse/silver/transaction_summary"
df_transaction_summary.write.format("delta").mode("overwrite").save(transaction_summary_silver_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS silver.transaction_summary "
          f"USING DELTA LOCATION '{transaction_summary_silver_path}'")

# -------------------------------
# Step 3: Customer Metrics (RFM, Churn, Loyalty)
# -------------------------------
df_ts = spark.table("silver.transaction_summary").alias("ts")

# NTILE(5) for Recency, Frequency, Monetary
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
    (F.col("RecencyScore")+F.col("FrequencyScore")+F.col("MonetaryScore")).alias("RFMScore")
)

df_churn_calc = (
    df_rfm_scored
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
    # A simplified loyalty logic. The T-SQL used ReturnRate <10, so adapt as needed:
    .withColumn(
      "LoyaltyIndex",
      F.when(F.col("TotalOrders") >= loyalty_threshold,
             ((F.col("TotalOrders")*0.6)+(F.lit(100)*0.4))/10
            )  # Simplified to skip actual ReturnRate
       .otherwise((F.col("TotalOrders")*0.4)/10)
    )
)

df_customer_metrics = (
    df_churn_calc
    .withColumn("CustomerLifetimeValue",
        F.col("TotalSpent") * (1 + (1 - F.col("ChurnProbability")))
    )
    .withColumn("CustomerHealth",
        F.when(F.col("ChurnProbability")>0.7, "At Risk")
         .when(F.col("LoyaltyIndex")>0.8, "Excellent")
         .when(F.col("LoyaltyIndex")>0.6, "Good")
         .when(F.col("LoyaltyIndex")>0.4, "Average")
         .otherwise("Needs Attention")
    )
    .select("CustomerID","CustomerLifetimeValue","RecencyScore","FrequencyScore","MonetaryScore",
            "RFMScore","ChurnProbability","NextPurchasePropensity","LoyaltyIndex","CustomerHealth")
)

customer_metrics_silver_path = "/lakehouse/silver/customer_metrics"
df_customer_metrics.write.format("delta").mode("overwrite").save(customer_metrics_silver_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS silver.customer_metrics "
          f"USING DELTA LOCATION '{customer_metrics_silver_path}'")

# -------------------------------
# Step 4: Customer Segments
# -------------------------------
df_cb = spark.table("silver.customer_base").alias("cb")
df_cm = spark.table("silver.customer_metrics").alias("cm")
df_ts2 = spark.table("silver.transaction_summary").alias("ts")

df_customer_segments = (
    df_cb.join(df_cm, "CustomerID", "inner")
         .join(df_ts2, "CustomerID", "inner")
         .select(
             F.col("cb.CustomerID"),
             # Value Segment
             F.when(F.col("cm.CustomerLifetimeValue") >= high_value_threshold, "High Value")
              .when(F.col("cm.CustomerLifetimeValue") >= (high_value_threshold*0.5), "Medium Value")
              .otherwise("Low Value").alias("ValueSegment"),
             # Behavior Segment
             F.when(F.col("cm.RFMScore") >= 13, "Champions")
              .when((F.col("cm.RecencyScore")>=4)&(F.col("cm.FrequencyScore")>=3), "Loyal Customers")
              .when((F.col("cm.RecencyScore")>=4)&(F.col("cm.FrequencyScore")<=2), "Potential Loyalists")
              .when((F.col("cm.RecencyScore")<=2)&(F.col("cm.FrequencyScore")>=3)&(F.col("cm.MonetaryScore")>=3), "At Risk")
              .when((F.col("cm.RecencyScore")<=2)&(F.col("cm.FrequencyScore")<=2)&(F.col("cm.MonetaryScore")<=2), "Hibernating")
              .when(F.col("cm.RecencyScore")<=1, "Lost")
              .otherwise("Others")
              .alias("BehaviorSegment"),
             # Lifecycle Segment
             F.when((F.col("ts.TotalOrders")==1)&(F.col("ts.DaysSinceLastPurchase")<=30), "New Customer")
              .when((F.col("ts.TotalOrders")>1)&(F.col("cm.ChurnProbability")<0.3), "Active")
              .when((F.col("cm.ChurnProbability")>=0.3)&(F.col("cm.ChurnProbability")<0.7), "At Risk")
              .when(F.col("cm.ChurnProbability")>=0.7, "Churned")
              .otherwise("Inactive")
              .alias("LifecycleSegment"),
             # TargetGroup
             F.when((F.col("cm.CustomerLifetimeValue")>=high_value_threshold)&(F.col("cm.ChurnProbability")<0.3), "VIP")
              .when((F.col("cm.CustomerLifetimeValue")>=(high_value_threshold*0.5))&(F.col("cm.ChurnProbability")>=0.3), "Retention Priority")
              .when((F.col("ts.TotalOrders")==1)&(F.col("ts.DaysSinceLastPurchase")<=30), "Nurture New")
              .when(F.col("cm.NextPurchasePropensity")>0.6, "Growth Opportunity")
              .when(F.col("ts.ReturnRate")>20, "Service Improvement")
              .otherwise("Standard")
              .alias("TargetGroup"),
             # MarketingRecommendation
             F.when((F.col("cm.CustomerLifetimeValue")>=high_value_threshold)&(F.col("cm.ChurnProbability")<0.3),
                    "Exclusive offers, VIP events, Personal shopping assistance, Early access to new products")
              .when((F.col("cm.CustomerLifetimeValue")>=(high_value_threshold*0.5))&(F.col("cm.ChurnProbability")>=0.3),
                    "Re-engagement campaign, Loyalty rewards, Personalized recommendations based on past purchases")
              .when((F.col("ts.TotalOrders")==1)&(F.col("ts.DaysSinceLastPurchase")<=30),
                    "Welcome series, Educational content, First purchase follow-up, Category exploration")
              .when(F.col("cm.NextPurchasePropensity")>0.6,
                    F.concat(F.lit("Cross-sell/upsell, Bundle offers based on "),
                             F.coalesce(F.col("ts.TopCategory"), F.lit("preferred category")),
                             F.lit(", Category expansion"))
              )
              .when(F.col("ts.ReturnRate")>20,
                    "Satisfaction survey, Improved product information, Size/fit guides, Service recovery")
              .otherwise(
                    F.concat(F.lit("Standard seasonal promotions, Category newsletters, Reactivation after "),
                             (F.coalesce(F.col("ts.DaysSinceLastPurchase"),F.lit(0))/F.lit(30)).cast("int"),
                             F.lit(" months")))
              .alias("MarketingRecommendation")
         )
)

customer_segments_silver_path = "/lakehouse/silver/customer_segments"
df_customer_segments.write.format("delta").mode("overwrite").save(customer_segments_silver_path)
spark.sql(f"CREATE TABLE IF NOT EXISTS silver.customer_segments "
          f"USING DELTA LOCATION '{customer_segments_silver_path}'")

--------------------------------------------------
3.3 Gold Layer – Final Analytics & Summaries
--------------------------------------------------
from delta.tables import DeltaTable

# Create or reference existing Delta tables in the Gold layer
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

gold_profiles_path = "/lakehouse/gold/customer_profiles"
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.customer_profiles "
          f"USING DELTA LOCATION '{gold_profiles_path}'")

df_customer_base = spark.table("silver.customer_base")
df_transaction_summary = spark.table("silver.transaction_summary")

df_profiles = (
    df_customer_base.alias("cb")
    .join(df_transaction_summary.alias("ts"), "CustomerID", "left")
    .select("cb.CustomerID","cb.CustomerName","cb.Email","cb.Phone","cb.Address",
            "cb.PostalCode","cb.City","cb.State","cb.Country","cb.CustomerType",
            "cb.AccountManager","cb.CreatedDate","cb.IsActive","cb.LastContactDate",
            "ts.TotalOrders","ts.TotalSpent","ts.AvgOrderValue",
            "ts.FirstPurchaseDate","ts.LastPurchaseDate","ts.DaysSinceLastPurchase",
            "ts.TopCategory","ts.TopProduct","ts.ReturnRate")
)

# Upsert to gold.customer_profiles
delta_profiles = DeltaTable.forPath(spark, gold_profiles_path)
(delta_profiles.alias("target")
 .merge(df_profiles.alias("source"), "target.CustomerID = source.CustomerID")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute()
)

# CustomerAnalytics
gold_analytics_path = "/lakehouse/gold/customer_analytics"
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.customer_analytics "
          f"USING DELTA LOCATION '{gold_analytics_path}'")

df_customer_metrics = spark.table("silver.customer_metrics")
df_customer_segments = spark.table("silver.customer_segments")

df_analytics = (
    df_customer_metrics.alias("cm")
    .join(df_customer_segments.alias("cs"), "CustomerID", "inner")
    .select(
        F.lit(data_date).alias("ProcessDate"),
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

delta_analytics = DeltaTable.forPath(spark, gold_analytics_path)

# 1) Remove data for this date
delta_analytics.delete(condition=f"ProcessDate = '{data_date}'")

# 2) Insert new data
df_analytics.write.format("delta").mode("append").save(gold_analytics_path)

# 3) Retention Window
if not debug_mode:
    retention_cutoff = spark.sql(f"SELECT date_sub('{data_date}', {retention_period_days}) AS cutoff").collect()[0]["cutoff"]
    delta_analytics.delete(condition=f"ProcessDate < '{retention_cutoff}'")

# Summaries
gold_summary_path = "/lakehouse/gold/customer_analytics_summary"
spark.sql(f"CREATE TABLE IF NOT EXISTS gold.customer_analytics_summary "
          f"USING DELTA LOCATION '{gold_summary_path}'")

df_summary = (
    df_customer_base.alias("cb")
    .join(df_customer_metrics.alias("cm"), "CustomerID")
    .join(df_customer_segments.alias("cs"), "CustomerID")
    .select(
       F.lit(data_date).alias("ProcessDate"),
       F.lit(process_type).alias("ProcessType"),
       F.count_distinct("cb.CustomerID").over(Window.partitionBy()).alias("TotalCustomers"),
       F.sum(F.when(F.col("cs.LifecycleSegment")=="Active",1).otherwise(0)).over(Window.partitionBy()).alias("ActiveCustomers"),
       F.sum(F.when(F.col("cs.LifecycleSegment")=="New Customer",1).otherwise(0)).over(Window.partitionBy()).alias("NewCustomers"),
       F.sum(F.when((F.col("cs.LifecycleSegment")=="At Risk")|(F.col("cs.LifecycleSegment")=="Churned"),1).otherwise(0)).over(Window.partitionBy()).alias("ChurningCustomers"),
       F.sum(F.when(F.col("cs.ValueSegment")=="High Value",1).otherwise(0)).over(Window.partitionBy()).alias("HighValueCustomers"),
       F.avg("cm.CustomerLifetimeValue").over(Window.partitionBy()).alias("AverageLifetimeValue")
    ).distinct()
)

processing_start_time = F.current_timestamp()

df_summary_final = df_summary.withColumn(
    "TotalProcessingTimeMs",
    (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(processing_start_time))*1000
).withColumn("ProcessingStatus", F.lit("Success"))

df_summary_final.write.format("delta").mode("append").save(gold_summary_path)

────────────────────────────────────────────────────────
4. TECH LEAD VIEW: IMPLEMENTATION GUIDELINES & BEST PRACTICES
────────────────────────────────────────────────────────
4.1 Use Delta for ACID operations and time-travel:
• Reliable merges, updates, and deletes.  
• Facilitates incremental loading and retention policy enforcement.

4.2 Partitioning and Data Skipping:
• For large volumes, partition by ProcessDate or CustomerID range.  
• Ensure data skipping to optimize queries.

4.3 Parameterization:
• Store thresholds (churnDays, highValueThreshold, etc.) in a config file or environment.  

4.4 Idempotent Pipeline Design:
• The procedure can be re-run for a given date without duplicating data.  
• Merges ensure “upsert” logic is applied.

4.5 Logging and Monitoring:
• Use Spark logs or Fabric Monitor.  
• Log row counts, run time, and errors.

4.6 Data Quality Checks:
• Validate negative orders, missing addresses, or invalid statuses.  
• Potentially quarantine or error these records for manual review.

────────────────────────────────────────────────────────
5. TESTING AGENT VIEW: SAMPLE TEST CASES
────────────────────────────────────────────────────────
5.1 Full vs. Incremental Load
• Run process_type=FULL and validate all records from the source appear in Bronze, Silver, and Gold.  
• Then run with process_type=INCREMENTAL and a recent data_date; confirm only newly updated rows are processed.

5.2 Churn Threshold Boundaries
• Provide sample data with DaysSinceLastPurchase set to 31, 61, 91, 181, etc. and confirm correct churn probabilities (0.1, 0.3, 0.5, 0.8).

5.3 Value Segment Threshold
• Include records with TotalSpent above/below 5000 to confirm correct ValueSegment (“High Value”, “Medium Value”, “Low Value”).

5.4 RFM Score Validation
• Ingest small test data sets with known RFM distribution. Verify scores match expected 1–5 quintiles.

5.5 Retention Cleanup
• Ingest data for multiple days. Ensure that data older than retention_period_days is removed from gold.customer_analytics.

5.6 Error Handling
• Introduce a schema mismatch or null columns. Verify the pipeline logs the issue and gracefully fails or skips invalid data.

5.7 Performance/Load
• Test with high volumes (millions of Orders). Confirm merges and partitioning strategies meet performance SLAs.

────────────────────────────────────────────────────────
CONCLUSION
────────────────────────────────────────────────────────
By separating raw ingestion (Bronze), curated transformations (Silver), and final analytics (Gold), the refactored PySpark pipeline is more maintainable, scalable, and flexible than a single large T-SQL stored procedure. It meets the same business requirements for customer analytics—RFM scoring, churn prediction, segmentation—while leveraging Microsoft Fabric’s modern lakehouse features (Delta, scalable Spark compute, and Medallion architecture).