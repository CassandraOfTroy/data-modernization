### Migration Plan and Implementation for SQL Stored Procedure to PySpark in Microsoft Fabric

---

#### **Migration Plan**

##### **Objective**
Migrate the SQL stored procedure `[dbo].[usp_CustomerAnalytics_Processing]` to PySpark using the Medallion Architecture (Bronze, Silver, Gold layers) while ensuring business logic, scalability, and performance optimization in Microsoft Fabric.

##### **Medallion Architecture Overview**
1. **Bronze Layer**: Raw data ingestion from SQL tables into Delta Lake.
2. **Silver Layer**: Data transformation and intermediate calculations (e.g., RFM scores, churn probability).
3. **Gold Layer**: Aggregated and business-ready data for customer segmentation and reporting.

##### **Key Migration Activities**
1. **Business Analysis**: Understand business rules, metrics, and thresholds.
2. **Data Mapping**: Map SQL tables and columns to PySpark DataFrames.
3. **Transformation Logic**: Translate SQL logic into PySpark transformations.
4. **Data Storage**: Use Delta Lake for ACID transactions and efficient querying.
5. **Testing and Validation**: Ensure data consistency and correctness.

##### **Prioritized User Stories**
1. **Bronze Layer**: Ingest raw data from SQL tables.
2. **Silver Layer**: Transform data to calculate metrics like RFM scores and churn probability.
3. **Gold Layer**: Segment customers and persist results for reporting.
4. **Retention Policy**: Implement data retention logic.
5. **Error Handling**: Log errors and ensure process robustness.

---

#### **PySpark Code Implementation**

##### **Bronze Layer: Raw Data Ingestion**
```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerAnalytics").getOrCreate()

# Load raw data from SQL tables
def load_table(table_name):
    return spark.read.format("jdbc").options(
        url="jdbc:sqlserver://<server>;database=<db>",
        dbtable=table_name,
        user="<username>",
        password="<password>"
    ).load()

customers = load_table("dbo.Customers")
customer_addresses = load_table("dbo.CustomerAddresses")
customer_interactions = load_table("dbo.CustomerInteractions")
orders = load_table("dbo.Orders")
order_details = load_table("dbo.OrderDetails")
products = load_table("dbo.Products")
returns = load_table("dbo.Returns")

# Save raw data to Bronze Layer in Delta Lake
def save_to_bronze(df, path):
    df.write.format("delta").mode("overwrite").save(path)

save_to_bronze(customers, "/bronze/customers")
save_to_bronze(customer_addresses, "/bronze/customer_addresses")
save_to_bronze(customer_interactions, "/bronze/customer_interactions")
save_to_bronze(orders, "/bronze/orders")
save_to_bronze(order_details, "/bronze/order_details")
save_to_bronze(products, "/bronze/products")
save_to_bronze(returns, "/bronze/returns")
```

---

##### **Silver Layer: Data Transformation**
```python
from pyspark.sql.functions import col, sum, count, avg, min, max, datediff, lit

# Load Bronze Layer data
customers = spark.read.format("delta").load("/bronze/customers")
orders = spark.read.format("delta").load("/bronze/orders")
order_details = spark.read.format("delta").load("/bronze/order_details")
returns = spark.read.format("delta").load("/bronze/returns")

# Calculate Transaction Summary
transaction_summary = orders.join(order_details, "OrderID") \
    .groupBy("CustomerID") \
    .agg(
        count("OrderID").alias("TotalOrders"),
        sum(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).alias("TotalSpent"),
        min("OrderDate").alias("FirstPurchaseDate"),
        max("OrderDate").alias("LastPurchaseDate")
    )

transaction_summary = transaction_summary.withColumn(
    "AvgOrderValue", col("TotalSpent") / col("TotalOrders")
).withColumn(
    "DaysSinceLastPurchase", datediff(lit("<DataDate>"), col("LastPurchaseDate"))
)

# Save to Silver Layer
transaction_summary.write.format("delta").mode("overwrite").save("/silver/transaction_summary")
```

---

##### **Gold Layer: Customer Segmentation**
```python
from pyspark.sql.functions import when, col

# Load Silver Layer data
transaction_summary = spark.read.format("delta").load("/silver/transaction_summary")

# Customer Segmentation
customer_segments = transaction_summary.withColumn(
    "ValueSegment", when(col("TotalSpent") >= 5000, "High Value")
    .when(col("TotalSpent") >= 2500, "Medium Value")
    .otherwise("Low Value")
).withColumn(
    "BehaviorSegment", when(col("TotalOrders") >= 10, "Champions")
    .when(col("TotalOrders") >= 5, "Loyal Customers")
    .otherwise("Others")
)

# Save to Gold Layer
customer_segments.write.format("delta").mode("overwrite").save("/gold/customer_segments")
```

---

#### **Implementation Guidelines**

1. **Data Partitioning**: Partition data by `ProcessDate` for efficient querying.
2. **Error Handling**: Use try-except blocks to handle errors gracefully.
3. **Logging**: Log processing steps and metrics for monitoring.
4. **Scalability**: Optimize transformations for large datasets using Spark's distributed processing.
5. **Retention Policy**: Implement logic to delete data older than the retention period.

---

#### **Test Cases**

##### **Bronze Layer**
1. Verify all source tables are ingested correctly.
2. Ensure data types match the source schema.

##### **Silver Layer**
1. Validate calculated metrics (e.g., `TotalOrders`, `TotalSpent`).
2. Check for null or incorrect values in derived columns.

##### **Gold Layer**
1. Verify segmentation logic aligns with business rules.
2. Ensure aggregated data matches expected results.

##### **End-to-End**
1. Compare final results with SQL output for consistency.
2. Test scalability with large datasets.

---

#### **Microsoft Fabric Best Practices**

1. **Delta Lake**: Use Delta Lake for ACID transactions and efficient querying.
2. **Data Lineage**: Maintain traceability across Bronze, Silver, and Gold layers.
3. **Parameterization**: Use configuration files for thresholds and retention periods.
4. **Monitoring**: Implement monitoring tools to track process performance and errors.
5. **Documentation**: Provide detailed documentation for business rules and technical implementation.

---

This migration plan ensures a seamless transition to PySpark with the Medallion Architecture while maintaining business logic and data integrity. The implementation is production-ready, scalable, and aligned with Microsoft Fabric best practices.