# BRONZE LAYER START
# ==================

# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Assume SparkSession is already created
# spark = SparkSession.builder.getOrCreate()

# Example parameters (replace with pipeline/Notebook widgets as needed)
process_date_str = "2023-10-01"                        # @DataDate
process_type = "FULL"                                  # @ProcessType
debug_mode = False                                     # @DebugMode
retention_period_days = 365                            # @RetentionPeriodDays
high_value_threshold = 5000.00
loyalty_threshold = 5
churn_days_very_high = 180
churn_days_high = 90
churn_days_medium = 60
churn_days_low = 30

# Convert string to date column
process_date = F.to_date(F.lit(process_date_str), "yyyy-MM-dd")

# 1) Read raw data into DataFrames (Bronze)
# Adjust paths or connections for your environment
df_customers_bronze = spark.read.format("delta").load("Tables/Bronze/Customers")
df_addresses_bronze = spark.read.format("delta").load("Tables/Bronze/CustomerAddresses")
df_interactions_bronze = spark.read.format("delta").load("Tables/Bronze/CustomerInteractions")
df_orders_bronze = spark.read.format("delta").load("Tables/Bronze/Orders")
df_orderdetails_bronze = spark.read.format("delta").load("Tables/Bronze/OrderDetails")
df_products_bronze = spark.read.format("delta").load("Tables/Bronze/Products")
df_returns_bronze = spark.read.format("delta").load("Tables/Bronze/Returns")

# These dataframes constitute the Bronze ingestion layer (raw data).
# BRONZE LAYER END