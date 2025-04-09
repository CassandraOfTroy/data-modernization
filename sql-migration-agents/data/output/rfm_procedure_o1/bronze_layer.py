# BRONZE LAYER START
# -------------------------------------------------------------
# This stage reads raw data from the source or raw tables.
# In Microsoft Fabric, these might be Lakehouse bronze tables.

from pyspark.sql import SparkSession, functions as F, Window
import datetime

spark = SparkSession.builder.getOrCreate()

# Example parameter defaults
dataDate = None
processType = 'FULL'
debugMode = False
retentionPeriodDays = 365
highValueThreshold = 5000.0
loyaltyThreshold = 5
churnDaysVeryHigh = 180
churnDaysHigh = 90
churnDaysMedium = 60
churnDaysLow = 30

# Set dataDate to yesterday if not provided
if dataDate is None:
    dataDate = datetime.date.today() - datetime.timedelta(days=1)

# Read raw (bronze) tables
customersBronzeDF = spark.read.table("lakehouse_raw.Customers")
addressesBronzeDF = spark.read.table("lakehouse_raw.CustomerAddresses")
interactionsBronzeDF = spark.read.table("lakehouse_raw.CustomerInteractions")
ordersBronzeDF = spark.read.table("lakehouse_raw.Orders")
orderDetailsBronzeDF = spark.read.table("lakehouse_raw.OrderDetails")
productsBronzeDF = spark.read.table("lakehouse_raw.Products")
returnsBronzeDF = spark.read.table("lakehouse_raw.Returns")

# BRONZE LAYER END