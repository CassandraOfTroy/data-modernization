from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from typing import Dict, Optional, List
import datetime

from ..utils.logging import get_logger
from ..utils.config import APP_CONFIG

logger = get_logger(__name__)

def get_gold_path(table_name: str) -> str:
    """Constructs the full ADLS path for a gold table."""
    adls_config = APP_CONFIG.get('adls', {})
    base_path = f"abfss://{adls_config.get('container_name')}@{adls_config.get('storage_account_name')}.dfs.core.windows.net"
    gold_path_prefix = adls_config.get('gold_path', '/gold')
    return f"{base_path}{gold_path_prefix}/{table_name}"

def transform_customer_metrics(
    spark: SparkSession,
    customer_base_df: DataFrame,
    transaction_summary_df: DataFrame,
    process_date: datetime.date,
    high_value_threshold: float = 5000.00,
    loyalty_threshold: int = 5,
    churn_days_very_high: int = 180,
    churn_days_high: int = 90,
    churn_days_medium: int = 60,
    churn_days_low: int = 30
) -> DataFrame:
    """Creates Gold CustomerMetrics DataFrame with RFM analysis and advanced metrics.
    
    Implements the logic equivalent to the #CustomerMetrics temp table in the stored procedure.
    
    Args:
        spark: The active SparkSession.
        customer_base_df: CustomerBase DataFrame from Silver layer.
        transaction_summary_df: TransactionSummary DataFrame from Silver layer.
        process_date: The processing date.
        high_value_threshold: Threshold for high-value customers (default: 5000.00).
        loyalty_threshold: Threshold for loyal customers (default: 5).
        churn_days_*: Thresholds for churn probability calculations.
    
    Returns:
        DataFrame containing CustomerMetrics data.
    """
    logger.info(f"Starting Gold CustomerMetrics transformation for date: {process_date}")
    
    # First, calculate RFM scores using ntile within windows
    # This is equivalent to the RFMScores CTE in the SQL procedure
    
    # Define the window for each RFM component
    recency_window = Window.orderBy(F.col("DaysSinceLastPurchase").asc_nulls_last())
    frequency_window = Window.orderBy(F.col("TotalOrders").desc_nulls_last())
    monetary_window = Window.orderBy(F.col("TotalSpent").desc_nulls_last())
    
    # Calculate RFM scores
    rfm_scores = (
        transaction_summary_df
        .withColumn("RecencyScore", F.ntile(5).over(recency_window))
        .withColumn("FrequencyScore", F.ntile(5).over(frequency_window))
        .withColumn("MonetaryScore", F.ntile(5).over(monetary_window))
        .withColumn("RFMScore", 
                    F.col("RecencyScore") + F.col("FrequencyScore") + F.col("MonetaryScore"))
        .select("CustomerID", "RecencyScore", "FrequencyScore", "MonetaryScore", "RFMScore")
    )
    
    # Now calculate churn probabilities and other metrics
    # This is equivalent to the ChurnCalculation CTE in the SQL procedure
    
    churn_calculation = (
        transaction_summary_df
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
            F.when((F.col("DaysSinceLastPurchase") < churn_days_low) & 
                   (F.col("TotalOrders") > 3), 0.8)
             .when((F.col("DaysSinceLastPurchase") < churn_days_medium) & 
                   (F.col("TotalOrders") > 2), 0.6)
             .when(F.col("DaysSinceLastPurchase") < churn_days_high, 0.4)
             .otherwise(0.2)
        )
        .withColumn(
            "LoyaltyIndex",
            F.when((F.col("TotalOrders") >= loyalty_threshold) & 
                   (F.col("ReturnRate") < 10),
                   (F.col("TotalOrders") * 0.6) + ((100 - F.col("ReturnRate")) * 0.4))
             .otherwise(F.col("TotalOrders") * 0.4)
             .divide(10)
        )
        .select("CustomerID", "ChurnProbability", "NextPurchasePropensity", "LoyaltyIndex")
    )
    
    # Now combine RFM scores with churn metrics and additional calculations
    # to create the customer metrics
    
    customer_metrics = (
        rfm_scores
        .join(churn_calculation, "CustomerID")
        .join(transaction_summary_df.select("CustomerID", "TotalSpent"), "CustomerID")
        # Calculate Customer Lifetime Value
        .withColumn(
            "CustomerLifetimeValue",
            F.col("TotalSpent") * (1 + (1 - F.col("ChurnProbability")))
        )
        # Calculate Customer Health
        .withColumn(
            "CustomerHealth",
            F.when(F.col("ChurnProbability") > 0.7, "At Risk")
             .when(F.col("LoyaltyIndex") > 0.8, "Excellent")
             .when(F.col("LoyaltyIndex") > 0.6, "Good")
             .when(F.col("LoyaltyIndex") > 0.4, "Average")
             .otherwise("Needs Attention")
        )
    )
    
    # Add gold metadata
    customer_metrics_with_metadata = customer_metrics.withColumn(
        "_gold_process_date", F.lit(process_date).cast("date")
    ).withColumn(
        "_gold_process_timestamp", F.current_timestamp()
    )
    
    # Write to Gold
    gold_path = get_gold_path("CustomerMetrics")
    logger.info(f"Writing CustomerMetrics to Gold path: {gold_path}")
    
    try:
        (customer_metrics_with_metadata.write
         .format("delta")
         .mode("overwrite")
         .partitionBy("_gold_process_date")
         .option("overwriteSchema", "true")
         .save(gold_path))
        logger.info(f"Successfully wrote CustomerMetrics to {gold_path}")
        
        # Return the DataFrame for subsequent transformations
        return customer_metrics_with_metadata
    except Exception as e:
        logger.error(f"Error writing CustomerMetrics to Gold: {e}", exc_info=True)
        raise

def transform_customer_segments(
    spark: SparkSession,
    customer_base_df: DataFrame,
    transaction_summary_df: DataFrame,
    customer_metrics_df: DataFrame,
    process_date: datetime.date,
    high_value_threshold: float = 5000.00
) -> DataFrame:
    """Creates Gold CustomerSegments DataFrame with segmentation and marketing recommendations.
    
    Implements the logic equivalent to the #CustomerSegments temp table in the stored procedure.
    
    Args:
        spark: The active SparkSession.
        customer_base_df: CustomerBase DataFrame from Silver layer.
        transaction_summary_df: TransactionSummary DataFrame from Silver layer.
        customer_metrics_df: CustomerMetrics DataFrame from Gold layer.
        process_date: The processing date.
        high_value_threshold: Threshold for high-value customers (default: 5000.00).
    
    Returns:
        DataFrame containing CustomerSegments data.
    """
    logger.info(f"Starting Gold CustomerSegments transformation for date: {process_date}")
    
    # Join the required data from the input DataFrames
    joined_data = (
        customer_base_df.select("CustomerID")
        .join(customer_metrics_df, "CustomerID")
        .join(transaction_summary_df, "CustomerID")
    )
    
    # Apply segmentation logic
    customer_segments = (
        joined_data
        # Value Segmentation
        .withColumn(
            "ValueSegment",
            F.when(F.col("CustomerLifetimeValue") >= high_value_threshold, "High Value")
             .when(F.col("CustomerLifetimeValue") >= high_value_threshold * 0.5, "Medium Value")
             .otherwise("Low Value")
        )
        
        # Behavior Segmentation
        .withColumn(
            "BehaviorSegment",
            F.when(F.col("RFMScore") >= 13, "Champions")
             .when((F.col("RecencyScore") >= 4) & (F.col("FrequencyScore") >= 3), "Loyal Customers")
             .when((F.col("RecencyScore") >= 4) & (F.col("FrequencyScore") <= 2), "Potential Loyalists")
             .when((F.col("RecencyScore") <= 2) & (F.col("FrequencyScore") >= 3) & 
                   (F.col("MonetaryScore") >= 3), "At Risk")
             .when((F.col("RecencyScore") <= 2) & (F.col("FrequencyScore") <= 2) & 
                   (F.col("MonetaryScore") <= 2), "Hibernating")
             .when(F.col("RecencyScore") <= 1, "Lost")
             .otherwise("Others")
        )
        
        # Lifecycle Segmentation
        .withColumn(
            "LifecycleSegment",
            F.when((F.col("TotalOrders") == 1) & (F.col("DaysSinceLastPurchase") <= 30), "New Customer")
             .when((F.col("TotalOrders") > 1) & (F.col("ChurnProbability") < 0.3), "Active")
             .when((F.col("ChurnProbability") >= 0.3) & (F.col("ChurnProbability") < 0.7), "At Risk")
             .when(F.col("ChurnProbability") >= 0.7, "Churned")
             .otherwise("Inactive")
        )
        
        # Target Group
        .withColumn(
            "TargetGroup",
            F.when((F.col("CustomerLifetimeValue") >= high_value_threshold) & 
                   (F.col("ChurnProbability") < 0.3), "VIP")
             .when((F.col("CustomerLifetimeValue") >= high_value_threshold * 0.5) & 
                   (F.col("ChurnProbability") >= 0.3), "Retention Priority")
             .when((F.col("TotalOrders") == 1) & (F.col("DaysSinceLastPurchase") <= 30), "Nurture New")
             .when(F.col("NextPurchasePropensity") > 0.6, "Growth Opportunity")
             .when(F.col("ReturnRate") > 20, "Service Improvement")
             .otherwise("Standard")
        )
    )
    
    # Add Marketing Recommendation based on segments
    customer_segments = (
        customer_segments
        .withColumn(
            "MarketingRecommendation",
            F.when((F.col("CustomerLifetimeValue") >= high_value_threshold) & 
                   (F.col("ChurnProbability") < 0.3),
                   "Exclusive offers, VIP events, Personal shopping assistance, Early access to new products")
             .when((F.col("CustomerLifetimeValue") >= high_value_threshold * 0.5) & 
                   (F.col("ChurnProbability") >= 0.3),
                   "Re-engagement campaign, Loyalty rewards, Personalized recommendations based on past purchases")
             .when((F.col("TotalOrders") == 1) & (F.col("DaysSinceLastPurchase") <= 30),
                   "Welcome series, Educational content, First purchase follow-up, Category exploration")
             .when(F.col("NextPurchasePropensity") > 0.6,
                   F.concat(F.lit("Cross-sell/upsell, Bundle offers based on "), 
                            F.coalesce(F.col("TopCategory"), F.lit("preferred category")), 
                            F.lit(", Category expansion")))
             .when(F.col("ReturnRate") > 20,
                   "Satisfaction survey, Improved product information, Size/fit guides, Service recovery")
             .otherwise(
                   F.concat(F.lit("Standard seasonal promotions, Category newsletters, Reactivation after "), 
                            (F.coalesce(F.col("DaysSinceLastPurchase"), F.lit(0)) / 30).cast("integer").cast("string"), 
                            F.lit(" months")))
        )
    )
    
    # Select only the relevant columns for the final output
    final_segments = customer_segments.select(
        "CustomerID",
        "ValueSegment",
        "BehaviorSegment",
        "LifecycleSegment",
        "TargetGroup",
        "MarketingRecommendation"
    )
    
    # Add gold metadata
    segments_with_metadata = final_segments.withColumn(
        "_gold_process_date", F.lit(process_date).cast("date")
    ).withColumn(
        "_gold_process_timestamp", F.current_timestamp()
    )
    
    # Write to Gold
    gold_path = get_gold_path("CustomerSegments")
    logger.info(f"Writing CustomerSegments to Gold path: {gold_path}")
    
    try:
        (segments_with_metadata.write
         .format("delta")
         .mode("overwrite")
         .partitionBy("_gold_process_date")
         .option("overwriteSchema", "true")
         .save(gold_path))
        logger.info(f"Successfully wrote CustomerSegments to {gold_path}")
        
        # Return the DataFrame for subsequent transformations
        return segments_with_metadata
    except Exception as e:
        logger.error(f"Error writing CustomerSegments to Gold: {e}", exc_info=True)
        raise

def generate_analytics_summary(
    spark: SparkSession,
    customer_base_df: DataFrame,
    customer_metrics_df: DataFrame,
    customer_segments_df: DataFrame,
    process_date: datetime.date,
    process_type: str,
    processing_start_time: datetime.datetime
) -> DataFrame:
    """Creates Gold CustomerAnalyticsSummary DataFrame with overall statistics.
    
    Implements the logic equivalent to the summary statistics inserted into 
    dbo.CustomerAnalyticsSummary in the stored procedure.
    
    Args:
        spark: The active SparkSession.
        customer_base_df: CustomerBase DataFrame from Silver layer.
        customer_metrics_df: CustomerMetrics DataFrame from Gold layer.
        customer_segments_df: CustomerSegments DataFrame from Gold layer.
        process_date: The processing date.
        process_type: The type of processing ('FULL' or 'INCREMENTAL').
        processing_start_time: The timestamp when the pipeline started.
    
    Returns:
        DataFrame containing CustomerAnalyticsSummary data.
    """
    logger.info(f"Generating Gold CustomerAnalyticsSummary for date: {process_date}")
    
    # Join all required data
    joined_data = (
        customer_base_df.select("CustomerID", "IsActive")
        .join(customer_metrics_df.select("CustomerID", "CustomerLifetimeValue"), "CustomerID")
        .join(customer_segments_df.select(
            "CustomerID", "ValueSegment", "LifecycleSegment"), "CustomerID")
    )
    
    # Aggregate statistics
    total_customers = joined_data.count()
    active_customers = joined_data.filter(F.col("LifecycleSegment") == "Active").count()
    new_customers = joined_data.filter(F.col("LifecycleSegment") == "New Customer").count()
    churning_customers = joined_data.filter(
        (F.col("LifecycleSegment") == "At Risk") | (F.col("LifecycleSegment") == "Churned")
    ).count()
    high_value_customers = joined_data.filter(F.col("ValueSegment") == "High Value").count()
    avg_lifetime_value = joined_data.agg(F.avg("CustomerLifetimeValue")).collect()[0][0]
    
    # Calculate processing time in milliseconds
    processing_time_ms = int((datetime.datetime.now() - processing_start_time).total_seconds() * 1000)
    
    # Create summary DataFrame
    summary_data = [(
        process_date,
        process_type,
        total_customers,
        active_customers,
        new_customers,
        churning_customers,
        high_value_customers,
        float(avg_lifetime_value) if avg_lifetime_value else 0.0,
        processing_time_ms,
        "Success"
    )]
    
    summary_schema = ["ProcessDate", "ProcessType", "TotalCustomers", "ActiveCustomers",
                      "NewCustomers", "ChurningCustomers", "HighValueCustomers",
                      "AverageLifetimeValue", "TotalProcessingTimeMs", "ProcessingStatus"]
    
    summary_df = spark.createDataFrame(summary_data, summary_schema)
    
    # Add gold metadata
    summary_with_metadata = summary_df.withColumn(
        "_gold_process_date", F.lit(process_date).cast("date")
    ).withColumn(
        "_gold_process_timestamp", F.current_timestamp()
    )
    
    # Write to Gold
    gold_path = get_gold_path("CustomerAnalyticsSummary")
    logger.info(f"Writing CustomerAnalyticsSummary to Gold path: {gold_path}")
    
    try:
        (summary_with_metadata.write
         .format("delta")
         .mode("append")  # Using append since we want to keep historical summaries
         .option("overwriteSchema", "true")
         .save(gold_path))
        logger.info(f"Successfully wrote CustomerAnalyticsSummary to {gold_path}")
        
        return summary_with_metadata
    except Exception as e:
        logger.error(f"Error writing CustomerAnalyticsSummary to Gold: {e}", exc_info=True)
        raise

def persist_customer_analytics(
    spark: SparkSession,
    customer_base_df: DataFrame,
    customer_metrics_df: DataFrame,
    customer_segments_df: DataFrame,
    process_date: datetime.date
) -> DataFrame:
    """Creates Gold CustomerAnalytics DataFrame with all analytics data for a given date.
    
    This combines metrics and segments to create a comprehensive analytics dataset,
    similar to what's inserted into dbo.CustomerAnalytics in the SQL procedure.
    
    Args:
        spark: The active SparkSession.
        customer_base_df: CustomerBase DataFrame from Silver layer.
        customer_metrics_df: CustomerMetrics DataFrame from Gold layer.
        customer_segments_df: CustomerSegments DataFrame from Gold layer.
        process_date: The processing date.
    
    Returns:
        DataFrame containing CustomerAnalytics data.
    """
    logger.info(f"Creating Gold CustomerAnalytics for date: {process_date}")
    
    # Join metrics and segments to create the analytics dataset
    customer_analytics = (
        customer_metrics_df.select(
            "CustomerID", "CustomerLifetimeValue", "RecencyScore", "FrequencyScore",
            "MonetaryScore", "RFMScore", "ChurnProbability", "NextPurchasePropensity",
            "LoyaltyIndex", "CustomerHealth"
        )
        .join(customer_segments_df.select(
            "CustomerID", "ValueSegment", "BehaviorSegment", 
            "LifecycleSegment", "TargetGroup", "MarketingRecommendation"), 
            "CustomerID")
    )
    
    # Add process date
    customer_analytics = customer_analytics.withColumn(
        "ProcessDate", F.lit(process_date).cast("date")
    )
    
    # Add gold metadata
    analytics_with_metadata = customer_analytics.withColumn(
        "_gold_process_date", F.lit(process_date).cast("date")
    ).withColumn(
        "_gold_process_timestamp", F.current_timestamp()
    )
    
    # Write to Gold
    gold_path = get_gold_path("CustomerAnalytics")
    logger.info(f"Writing CustomerAnalytics to Gold path: {gold_path}")
    
    try:
        # First, delete any existing entries for this process date (equivalent to the DELETE in SQL)
        existing_analytics = spark.read.format("delta").load(gold_path)
        if existing_analytics.count() > 0:
            logger.info(f"Deleting existing CustomerAnalytics records for date: {process_date}")
            # Use Delta Lake's ability to delete with predicate
            spark.sql(f"""
                DELETE FROM delta.`{gold_path}`
                WHERE ProcessDate = '{process_date}'
            """)
        
        # Now insert the new analytics
        (analytics_with_metadata.write
         .format("delta")
         .mode("append")
         .option("overwriteSchema", "true")
         .save(gold_path))
        logger.info(f"Successfully wrote CustomerAnalytics to {gold_path}")
        
        return analytics_with_metadata
    except Exception as e:
        logger.error(f"Error writing CustomerAnalytics to Gold: {e}", exc_info=True)
        raise

def run_gold_transformation(
    spark: SparkSession,
    silver_dfs: Dict[str, DataFrame],
    process_date: datetime.date,
    process_type: str,
    processing_start_time: datetime.datetime,
    high_value_threshold: float = 5000.00,
    loyalty_threshold: int = 5,
    churn_days_very_high: int = 180,
    churn_days_high: int = 90,
    churn_days_medium: int = 60,
    churn_days_low: int = 30,
    retention_period_days: int = 365,
    debug_mode: bool = False
) -> Dict[str, DataFrame]:
    """Runs the Gold layer transformations for the pipeline.
    
    Args:
        spark: The active SparkSession.
        silver_dfs: Dictionary of silver DataFrames.
        process_date: The processing date.
        process_type: "FULL" or "INCREMENTAL".
        processing_start_time: When the pipeline started.
        high_value_threshold: Threshold for high-value customers.
        loyalty_threshold: Threshold for loyal customers.
        churn_days_*: Thresholds for churn probability calculations.
        retention_period_days: Number of days to retain historical data.
        debug_mode: Whether to run in debug mode.
    
    Returns:
        Dictionary containing the gold DataFrames.
    """
    gold_dfs = {}
    
    # Extract silver DataFrames
    customer_base_df = silver_dfs.get("CustomerBase")
    transaction_summary_df = silver_dfs.get("TransactionSummary")
    
    if not all([customer_base_df, transaction_summary_df]):
        missing = []
        if not customer_base_df: missing.append("CustomerBase")
        if not transaction_summary_df: missing.append("TransactionSummary")
        logger.error(f"Missing required silver DataFrames: {', '.join(missing)}")
        raise ValueError(f"Missing required silver DataFrames: {', '.join(missing)}")
    
    # Step 1: Transform CustomerMetrics
    customer_metrics_df = transform_customer_metrics(
        spark, customer_base_df, transaction_summary_df, process_date,
        high_value_threshold, loyalty_threshold,
        churn_days_very_high, churn_days_high, churn_days_medium, churn_days_low
    )
    gold_dfs["CustomerMetrics"] = customer_metrics_df
    
    # Step 2: Transform CustomerSegments
    customer_segments_df = transform_customer_segments(
        spark, customer_base_df, transaction_summary_df, customer_metrics_df,
        process_date, high_value_threshold
    )
    gold_dfs["CustomerSegments"] = customer_segments_df
    
    # Step 3: Persist CustomerAnalytics
    customer_analytics_df = persist_customer_analytics(
        spark, customer_base_df, customer_metrics_df, customer_segments_df, process_date
    )
    gold_dfs["CustomerAnalytics"] = customer_analytics_df
    
    # Step 4: Generate Summary
    summary_df = generate_analytics_summary(
        spark, customer_base_df, customer_metrics_df, customer_segments_df,
        process_date, process_type, processing_start_time
    )
    gold_dfs["CustomerAnalyticsSummary"] = summary_df
    
    # Step 5: Cleanup old data if not in debug mode
    if not debug_mode:
        cleanup_cutoff_date = process_date - datetime.timedelta(days=retention_period_days)
        logger.info(f"Cleaning up CustomerAnalytics data older than: {cleanup_cutoff_date}")
        
        try:
            analytics_path = get_gold_path("CustomerAnalytics")
            spark.sql(f"""
                DELETE FROM delta.`{analytics_path}`
                WHERE ProcessDate < '{cleanup_cutoff_date}'
            """)
            logger.info(f"Successfully cleaned up old CustomerAnalytics data")
        except Exception as e:
            logger.warning(f"Error cleaning up old CustomerAnalytics data: {e}")
    
    return gold_dfs 