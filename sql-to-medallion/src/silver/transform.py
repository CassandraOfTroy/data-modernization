from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from typing import Dict, Optional, Tuple
import datetime

from ..utils.logging import get_logger
from ..utils.config import APP_CONFIG

logger = get_logger(__name__)

def get_silver_path(table_name: str) -> str:
    """Constructs the full ADLS path for a silver table."""
    adls_config = APP_CONFIG.get('adls', {})
    base_path = f"abfss://{adls_config.get('container_name')}@{adls_config.get('storage_account_name')}.dfs.core.windows.net"
    silver_path_prefix = adls_config.get('silver_path', '/silver')
    return f"{base_path}{silver_path_prefix}/{table_name}"

def transform_customer_base(
    spark: SparkSession,
    bronze_dfs: Dict[str, DataFrame],
    process_date: datetime.date,
    process_type: str = "FULL"
) -> DataFrame:
    """Creates the Silver CustomerBase DataFrame by joining and transforming customer data.
    
    Implements the logic equivalent to the #CustomerBase temp table in the stored procedure.
    
    Args:
        spark: The active SparkSession.
        bronze_dfs: Dictionary of bronze DataFrames (should contain 'dbo.Customers', 
                   'dbo.CustomerAddresses', and 'dbo.CustomerInteractions').
        process_date: The processing date.
        process_type: "FULL" or "INCREMENTAL".
    
    Returns:
        DataFrame containing the transformed customer base data.
    """
    logger.info(f"Starting Silver CustomerBase transformation for date: {process_date}")
    
    # Extract needed DataFrames from bronze_dfs
    customers_df = bronze_dfs.get("dbo.Customers")
    addresses_df = bronze_dfs.get("dbo.CustomerAddresses")
    interactions_df = bronze_dfs.get("dbo.CustomerInteractions")
    
    if not all([customers_df, addresses_df, interactions_df]):
        missing = []
        if not customers_df: missing.append("dbo.Customers")
        if not addresses_df: missing.append("dbo.CustomerAddresses")
        if not interactions_df: missing.append("dbo.CustomerInteractions")
        logger.error(f"Missing required bronze DataFrames: {', '.join(missing)}")
        raise ValueError(f"Missing required bronze DataFrames: {', '.join(missing)}")
    
    # 1. Select and join data from customer, addresses, and interactions
    # This follows the logic from #CustomerBase in SQL:
    # - Join Customer with primary addresses
    # - Aggregate interactions to get LastContactDate
    
    # Filter and prepare addresses DataFrame (only primary addresses)
    primary_addresses = addresses_df.where(F.col("AddressType") == "Primary")
    
    # Calculate LastContactDate using window function similar to the MAX() in SQL
    interaction_window = Window.partitionBy("CustomerID")
    interactions_with_max = interactions_df.withColumn(
        "LastContactDate", 
        F.max("ContactDate").over(interaction_window)
    ).select("CustomerID", "LastContactDate").distinct()
    
    # Join customers with addresses and interactions
    customer_base = (
        customers_df
        .select(
            "CustomerID",
            F.concat_ws(" ", F.col("FirstName"), F.col("LastName")).alias("CustomerName"),
            "Email",
            "Phone",
            "CustomerType",
            F.col("AccountManagerID").alias("AccountManager"),
            "CreatedDate",
            "ModifiedDate",
            F.when(F.col("Status") == "Active", F.lit(1)).otherwise(F.lit(0)).alias("IsActive")
        )
        # Left join with addresses to get primary address info
        .join(
            primary_addresses.select(
                "CustomerID",
                F.col("StreetAddress").alias("Address"),
                "PostalCode",
                "City",
                "State",
                "Country"
            ),
            "CustomerID",
            "left"
        )
        # Left join with interactions to get LastContactDate
        .join(
            interactions_with_max,
            "CustomerID",
            "left"
        )
    )
    
    # Apply incremental filtering if needed (similar to WHERE condition in SQL)
    if process_type != "FULL":
        modified_date = process_date
        customer_base = customer_base.where(
            (F.col("ModifiedDate") >= modified_date) | 
            (F.col("LastContactDate") >= modified_date)
        )
    
    # Add silver metadata
    customer_base_with_metadata = customer_base.withColumn(
        "_silver_process_date", F.lit(process_date).cast("date")
    ).withColumn(
        "_silver_process_timestamp", F.current_timestamp()
    )
    
    # Write to Silver
    silver_path = get_silver_path("CustomerBase")
    logger.info(f"Writing CustomerBase to Silver path: {silver_path}")
    
    try:
        (customer_base_with_metadata.write
         .format("delta")
         .mode("overwrite")
         .partitionBy("_silver_process_date")
         .option("overwriteSchema", "true")
         .save(silver_path))
        logger.info(f"Successfully wrote CustomerBase to {silver_path}")
        
        # Return the DataFrame for subsequent transformations
        return customer_base_with_metadata
    except Exception as e:
        logger.error(f"Error writing CustomerBase to Silver: {e}", exc_info=True)
        raise

def transform_transaction_summary(
    spark: SparkSession,
    bronze_dfs: Dict[str, DataFrame],
    customer_base_df: DataFrame,
    process_date: datetime.date
) -> DataFrame:
    """Creates the Silver TransactionSummary DataFrame by analyzing order and return data.
    
    Implements the logic equivalent to the #TransactionSummary temp table in the stored procedure.
    
    Args:
        spark: The active SparkSession.
        bronze_dfs: Dictionary of bronze DataFrames (should contain 'dbo.Orders', 
                   'dbo.OrderDetails', 'dbo.Products', and 'dbo.Returns').
        customer_base_df: The CustomerBase DataFrame from the previous step.
        process_date: The processing date.
    
    Returns:
        DataFrame containing the transaction summary data.
    """
    logger.info(f"Starting Silver TransactionSummary transformation for date: {process_date}")
    
    # Extract needed DataFrames from bronze_dfs
    orders_df = bronze_dfs.get("dbo.Orders")
    order_details_df = bronze_dfs.get("dbo.OrderDetails")
    products_df = bronze_dfs.get("dbo.Products")
    returns_df = bronze_dfs.get("dbo.Returns")
    
    if not all([orders_df, order_details_df, products_df, returns_df]):
        missing = []
        if not orders_df: missing.append("dbo.Orders")
        if not order_details_df: missing.append("dbo.OrderDetails")
        if not products_df: missing.append("dbo.Products")
        if not returns_df: missing.append("dbo.Returns")
        logger.error(f"Missing required bronze DataFrames: {', '.join(missing)}")
        raise ValueError(f"Missing required bronze DataFrames: {', '.join(missing)}")
    
    # 1. Calculate base order metrics (equivalent to OrderSummary CTE)
    # Filter completed orders only
    completed_orders = orders_df.where(F.col("Status") == "Completed")
    
    # Join orders with order details and calculate metrics
    order_summary = (
        completed_orders
        .join(order_details_df, "OrderID")
        .groupBy("CustomerID")
        .agg(
            F.countDistinct("OrderID").alias("TotalOrders"),
            F.sum(
                order_details_df["Quantity"] * 
                order_details_df["UnitPrice"] * 
                (1 - order_details_df["Discount"])
            ).alias("TotalSpent"),
            F.min("OrderDate").alias("FirstPurchaseDate"),
            F.max("OrderDate").alias("LastPurchaseDate")
        )
    )
    
    # 2. Calculate additional metrics like AvgOrderValue and DaysSinceLastPurchase
    transaction_summary = (
        order_summary
        .withColumn(
            "AvgOrderValue",
            F.when(F.col("TotalOrders") > 0, F.col("TotalSpent") / F.col("TotalOrders"))
            .otherwise(F.lit(0))
        )
        .withColumn(
            "DaysSinceLastPurchase",
            F.datediff(F.lit(process_date), F.col("LastPurchaseDate"))
        )
        # Initialize columns that will be updated later
        .withColumn("TopCategory", F.lit(None).cast("string"))
        .withColumn("TopProduct", F.lit(None).cast("string"))
        .withColumn("ReturnRate", F.lit(0.0).cast("double"))
    )
    
    # 3. Calculate top category per customer
    category_spend = (
        completed_orders
        .join(order_details_df, "OrderID")
        .join(products_df, "ProductID")
        .groupBy("CustomerID", "Category")
        .agg(
            F.sum(
                order_details_df["Quantity"] * 
                order_details_df["UnitPrice"] * 
                (1 - order_details_df["Discount"])
            ).alias("CategorySpend")
        )
    )
    
    # Use window function to rank categories by spend
    category_window = Window.partitionBy("CustomerID").orderBy(F.desc("CategorySpend"))
    top_categories = (
        category_spend
        .withColumn("CategoryRank", F.row_number().over(category_window))
        .where(F.col("CategoryRank") == 1)
        .select("CustomerID", "Category")
    )
    
    # Update transaction summary with top category
    transaction_summary = transaction_summary.join(
        top_categories, 
        "CustomerID", 
        "left"
    ).withColumn(
        "TopCategory",
        F.when(F.col("Category").isNotNull(), F.col("Category"))
        .otherwise(F.col("TopCategory"))
    ).drop("Category")
    
    # 4. Calculate top product per customer
    product_quantity = (
        completed_orders
        .join(order_details_df, "OrderID")
        .join(products_df, "ProductID")
        .groupBy("CustomerID", "ProductName")
        .agg(F.sum("Quantity").alias("TotalQuantity"))
    )
    
    # Use window function to rank products by quantity
    product_window = Window.partitionBy("CustomerID").orderBy(F.desc("TotalQuantity"))
    top_products = (
        product_quantity
        .withColumn("ProductRank", F.row_number().over(product_window))
        .where(F.col("ProductRank") == 1)
        .select("CustomerID", "ProductName")
    )
    
    # Update transaction summary with top product
    transaction_summary = transaction_summary.join(
        top_products, 
        "CustomerID", 
        "left"
    ).withColumn(
        "TopProduct",
        F.when(F.col("ProductName").isNotNull(), F.col("ProductName"))
        .otherwise(F.col("TopProduct"))
    ).drop("ProductName")
    
    # 5. Calculate return rate
    returns_summary = (
        returns_df
        .join(transaction_summary, "CustomerID")
        .groupBy("CustomerID", "TotalOrders")
        .agg(F.countDistinct("ReturnID").alias("TotalReturns"))
        .withColumn(
            "ReturnRate",
            F.when(F.col("TotalOrders") > 0,
                 (F.col("TotalReturns").cast("double") / F.col("TotalOrders").cast("double")) * 100)
            .otherwise(F.lit(0.0))
        )
        .select("CustomerID", "ReturnRate")
    )
    
    # Update transaction summary with return rate
    transaction_summary = transaction_summary.join(
        returns_summary, 
        "CustomerID", 
        "left"
    ).withColumn(
        "ReturnRate",
        F.coalesce(F.col("ReturnRate.ReturnRate"), F.col("ReturnRate"))
    )
    
    # 6. Filter to only include customers in the CustomerBase
    transaction_summary = transaction_summary.join(
        customer_base_df.select("CustomerID"),
        "CustomerID",
        "inner"
    )
    
    # Add silver metadata
    transaction_summary_with_metadata = transaction_summary.withColumn(
        "_silver_process_date", F.lit(process_date).cast("date")
    ).withColumn(
        "_silver_process_timestamp", F.current_timestamp()
    )
    
    # Write to Silver
    silver_path = get_silver_path("TransactionSummary")
    logger.info(f"Writing TransactionSummary to Silver path: {silver_path}")
    
    try:
        (transaction_summary_with_metadata.write
         .format("delta")
         .mode("overwrite")
         .partitionBy("_silver_process_date")
         .option("overwriteSchema", "true")
         .save(silver_path))
        logger.info(f"Successfully wrote TransactionSummary to {silver_path}")
        
        # Return the DataFrame for subsequent transformations
        return transaction_summary_with_metadata
    except Exception as e:
        logger.error(f"Error writing TransactionSummary to Silver: {e}", exc_info=True)
        raise

def run_silver_transformation(
    spark: SparkSession,
    bronze_dfs: Dict[str, DataFrame],
    process_date: datetime.date,
    process_type: str = "FULL"
) -> Dict[str, DataFrame]:
    """Runs the Silver transformations for the pipeline.
    
    Args:
        spark: The active SparkSession.
        bronze_dfs: Dictionary of bronze DataFrames.
        process_date: The processing date.
        process_type: "FULL" or "INCREMENTAL".
    
    Returns:
        Dictionary containing the silver DataFrames.
    """
    silver_dfs = {}
    
    # Step 1: Transform CustomerBase
    customer_base_df = transform_customer_base(
        spark, bronze_dfs, process_date, process_type
    )
    silver_dfs["CustomerBase"] = customer_base_df
    
    # Step 2: Transform TransactionSummary
    transaction_summary_df = transform_transaction_summary(
        spark, bronze_dfs, customer_base_df, process_date
    )
    silver_dfs["TransactionSummary"] = transaction_summary_df
    
    return silver_dfs 