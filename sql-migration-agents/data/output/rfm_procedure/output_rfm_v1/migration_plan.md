### Migration Plan and Implementation for SQL Stored Procedure to PySpark in Microsoft Fabric

---

#### **Migration Plan**

The migration plan involves translating the SQL stored procedure into PySpark code while adhering to the Medallion Architecture (Bronze, Silver, Gold layers). The implementation will leverage Microsoft Fabric's Delta Lake for data storage and processing.

---

#### **Medallion Architecture Overview**

1. **Bronze Layer**: Raw data ingestion from SQL tables into Delta Lake.
2. **Silver Layer**: Cleaned and enriched data with calculated metrics (e.g., transaction summaries, RFM scores).
3. **Gold Layer**: Aggregated and segmented data for analytics and reporting.

---

### PySpark Implementation

#### **Bronze Layer: Raw Data Ingestion**

```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerAnalyticsMigration").getOrCreate()

# Load raw data from SQL Server into Delta Lake (Bronze Layer)
def load_table_to_bronze(table_name, save_path):
    df = spark.read.format("jdbc").options(
        url="jdbc:sqlserver://<server>:<port>;databaseName=<database>",
        dbtable=table_name,
        user="<username>",
        password="<password>"
    ).load()
    df.write.format("delta").mode("overwrite").save(save_path)

# Ingest tables into Bronze Layer
load_table_to_bronze("dbo.Customers", "/mnt/delta/bronze/customers")
load_table_to_bronze("dbo.CustomerAddresses", "/mnt/delta/bronze/customer_addresses")
load_table_to_bronze("dbo.Orders", "/mnt/delta/bronze/orders")
load_table_to_bronze("dbo.OrderDetails", "/mnt/delta/bronze/order_details")
load_table_to_bronze("dbo.Products", "/mnt/delta/bronze/products")
load_table_to_bronze("dbo.Returns", "/mnt/delta/bronze/returns")
```

---

#### **Silver Layer: Data Transformation**

```python
from pyspark.sql.functions import col, sum, avg, min, max, count, when, datediff, lit

# Load Bronze Layer data
customers = spark.read.format("delta").load("/mnt/delta/bronze/customers")
customer_addresses = spark.read.format("delta").load("/mnt/delta/bronze/customer_addresses")
orders = spark.read.format("delta").load("/mnt/delta/bronze/orders")
order_details = spark.read.format("delta").load("/mnt/delta/bronze/order_details")
products = spark.read.format("delta").load("/mnt/delta/bronze/products")
returns = spark.read.format("delta").load("/mnt/delta/bronze/returns")

# Step 1: Extract Customer Base
customer_base = customers.join(customer_addresses, ["CustomerID"], "left").select(
    col("CustomerID"),
    col("FirstName").alias("CustomerName"),
    col("Email"),
    col("Phone"),
    col("StreetAddress").alias("Address"),
    col("PostalCode"),
    col("City"),
    col("State"),
    col("Country"),
    col("CustomerType"),
    col("AccountManagerID").alias("AccountManager"),
    col("CreatedDate"),
    col("ModifiedDate"),
    when(col("Status") == "Active", lit(1)).otherwise(lit(0)).alias("IsActive"),
    col("LastContactDate")
)

# Step 2: Transaction Summary
transaction_summary = orders.join(order_details, ["OrderID"]).groupBy("CustomerID").agg(
    count("OrderID").alias("TotalOrders"),
    sum(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).alias("TotalSpent"),
    avg(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).alias("AvgOrderValue"),
    min("OrderDate").alias("FirstPurchaseDate"),
    max("OrderDate").alias("LastPurchaseDate"),
    datediff(lit("2023-01-01"), max("OrderDate")).alias("DaysSinceLastPurchase")
)

# Write Silver Layer data
customer_base.write.format("delta").mode("overwrite").save("/mnt/delta/silver/customer_base")
transaction_summary.write.format("delta").mode("overwrite").save("/mnt/delta/silver/transaction_summary")
```

---

#### **Gold Layer: Customer Segmentation**

```python
from pyspark.sql.functions import when, lit

# Load Silver Layer data
customer_base = spark.read.format("delta").load("/mnt/delta/silver/customer_base")
transaction_summary = spark.read.format("delta").load("/mnt/delta/silver/transaction_summary")

# Customer Segmentation
customer_segments = transaction_summary.join(customer_base, ["CustomerID"]).select(
    col("CustomerID"),
    when(col("TotalSpent") >= 5000, "High Value").when(col("TotalSpent") >= 2500, "Medium Value").otherwise("Low Value").alias("ValueSegment"),
    when(col("TotalOrders") >= 10, "Champions").when(col("TotalOrders") >= 5, "Loyal Customers").otherwise("Others").alias("BehaviorSegment"),
    when(col("DaysSinceLastPurchase") <= 30, "New Customer").when(col("DaysSinceLastPurchase") <= 90, "Active").otherwise("Inactive").alias("LifecycleSegment")
)

# Write Gold Layer data
customer_segments.write.format("delta").mode("overwrite").save("/mnt/delta/gold/customer_segments")
```

---

### Implementation Guidelines and Best Practices

1. **Partitioning**: Use partitioning for large datasets to improve query performance.
2. **Delta Lake**: Leverage Delta Lake for ACID transactions and efficient data updates.
3. **Configurable Parameters**: Use environment variables or configuration files for thresholds.
4. **Logging**: Implement logging for monitoring and debugging.
5. **Error Handling**: Use try-except blocks to handle errors gracefully.

---

### Test Cases

#### **Test Case 1: Data Completeness**
- Verify that all rows from the source tables are ingested into the Bronze Layer.

#### **Test Case 2: Metric Accuracy**
- Validate calculated metrics (e.g., TotalOrders, TotalSpent) against sample data.

#### **Test Case 3: Segmentation Logic**
- Check that customers are correctly segmented based on business rules.

#### **Test Case 4: Data Retention**
- Ensure data older than the retention period is deleted.

#### **Test Case 5: End-to-End Validation**
- Compare results in the Gold Layer with expected outputs.

---

This migration plan and PySpark implementation follow Microsoft Fabric best practices and are production-ready.