### Migration Plan Execution

#### **1. Bronze Layer: Raw Data Ingestion**
The Bronze layer ingests raw data from SQL Server tables into Delta Lake tables in Microsoft Fabric. This layer ensures that the raw data is preserved for traceability and debugging.

```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerAnalytics").getOrCreate()

# JDBC connection properties
jdbc_url = "jdbc:sqlserver://<server>;database=<database>"
connection_properties = {
    "user": "<username>",
    "password": "<password>",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Load raw data from SQL Server
customers_df = spark.read.jdbc(url=jdbc_url, table="dbo.Customers", properties=connection_properties)
orders_df = spark.read.jdbc(url=jdbc_url, table="dbo.Orders", properties=connection_properties)
order_details_df = spark.read.jdbc(url=jdbc_url, table="dbo.OrderDetails", properties=connection_properties)
products_df = spark.read.jdbc(url=jdbc_url, table="dbo.Products", properties=connection_properties)
returns_df = spark.read.jdbc(url=jdbc_url, table="dbo.Returns", properties=connection_properties)

# Write raw data to Delta Lake
customers_df.write.format("delta").mode("overwrite").save("/bronze/customers")
orders_df.write.format("delta").mode("overwrite").save("/bronze/orders")
order_details_df.write.format("delta").mode("overwrite").save("/bronze/order_details")
products_df.write.format("delta").mode("overwrite").save("/bronze/products")
returns_df.write.format("delta").mode("overwrite").save("/bronze/returns")
```

---

#### **2. Silver Layer: Intermediate Transformations**
The Silver layer processes and enriches the raw data to calculate intermediate metrics like transaction summaries and RFM scores.

```python
from pyspark.sql.functions import col, sum, avg, min, max, countDistinct, datediff, current_date

# Load raw data from Bronze Layer
customers_bronze = spark.read.format("delta").load("/bronze/customers")
orders_bronze = spark.read.format("delta").load("/bronze/orders")
order_details_bronze = spark.read.format("delta").load("/bronze/order_details")
products_bronze = spark.read.format("delta").load("/bronze/products")
returns_bronze = spark.read.format("delta").load("/bronze/returns")

# Join orders and order details to calculate transaction metrics
transaction_summary = orders_bronze.join(order_details_bronze, "OrderID") \
    .groupBy("CustomerID").agg(
        countDistinct("OrderID").alias("TotalOrders"),
        sum(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).alias("TotalSpent"),
        avg(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).alias("AvgOrderValue"),
        min("OrderDate").alias("FirstPurchaseDate"),
        max("OrderDate").alias("LastPurchaseDate"),
        datediff(current_date(), max("OrderDate")).alias("DaysSinceLastPurchase")
    )

# Write transaction summary to Delta Lake
transaction_summary.write.format("delta").mode("overwrite").save("/silver/transaction_summary")
```

---

#### **3. Gold Layer: Advanced Calculations and Segmentation**
The Gold layer performs advanced calculations, such as RFM scoring, churn probability, and customer segmentation.

```python
from pyspark.sql.functions import when

# Load data from Silver Layer
transaction_summary_silver = spark.read.format("delta").load("/silver/transaction_summary")

# Calculate RFM scores
rfm_scores = transaction_summary_silver.withColumn(
    "RecencyScore", when(col("DaysSinceLastPurchase") <= 30, 5)
                    .when(col("DaysSinceLastPurchase") <= 60, 4)
                    .when(col("DaysSinceLastPurchase") <= 90, 3)
                    .when(col("DaysSinceLastPurchase") <= 180, 2)
                    .otherwise(1)
).withColumn(
    "FrequencyScore", when(col("TotalOrders") >= 10, 5)
                      .when(col("TotalOrders") >= 5, 4)
                      .when(col("TotalOrders") >= 3, 3)
                      .when(col("TotalOrders") >= 1, 2)
                      .otherwise(1)
).withColumn(
    "MonetaryScore", when(col("TotalSpent") >= 5000, 5)
                     .when(col("TotalSpent") >= 2500, 4)
                     .when(col("TotalSpent") >= 1000, 3)
                     .when(col("TotalSpent") >= 500, 2)
                     .otherwise(1)
).withColumn(
    "RFMScore", col("RecencyScore") + col("FrequencyScore") + col("MonetaryScore")
).withColumn(
    "ChurnProbability", when(col("DaysSinceLastPurchase") > 180, 0.8)
                        .when(col("DaysSinceLastPurchase") > 90, 0.5)
                        .when(col("DaysSinceLastPurchase") > 60, 0.3)
                        .when(col("DaysSinceLastPurchase") > 30, 0.1)
                        .otherwise(0.05)
)

# Write RFM scores to Delta Lake
rfm_scores.write.format("delta").mode("overwrite").save("/gold/rfm_scores")

# Perform customer segmentation
customer_segments = rfm_scores.withColumn(
    "ValueSegment", when(col("TotalSpent") >= 5000, "High Value")
                    .when(col("TotalSpent") >= 2500, "Medium Value")
                    .otherwise("Low Value")
).withColumn(
    "BehaviorSegment", when(col("RFMScore") >= 13, "Champions")
                       .when((col("RecencyScore") >= 4) & (col("FrequencyScore") >= 3), "Loyal Customers")
                       .when((col("RecencyScore") >= 4) & (col("FrequencyScore") <= 2), "Potential Loyalists")
                       .when((col("RecencyScore") <= 2) & (col("FrequencyScore") >= 3), "At Risk")
                       .when((col("RecencyScore") <= 2) & (col("FrequencyScore") <= 2), "Hibernating")
                       .otherwise("Others")
)

# Write customer segments to Delta Lake
customer_segments.write.format("delta").mode("overwrite").save("/gold/customer_segments")
```

---

### Implementation Guidelines and Best Practices
1. **Delta Lake**:
   - Use Delta Lake for all layers to ensure ACID compliance and efficient updates.
2. **Partitioning**:
   - Partition data by `ProcessDate` for efficient querying and incremental processing.
3. **Error Handling**:
   - Use `try-except` blocks to handle errors and log processing steps.
4. **Scalability**:
   - Use Spark's distributed processing capabilities to handle large datasets efficiently.
5. **Documentation**:
   - Add comments to explain business logic and transformations.

---

### Test Cases

#### **Test Case 1: Data Integrity**
- **Objective**: Verify that raw data in the Bronze layer matches the source SQL tables.
- **Validation**: Compare row counts and column values.

#### **Test Case 2: Metric Accuracy**
- **Objective**: Ensure that metrics (e.g., TotalOrders, RFM scores) are calculated correctly.
- **Validation**: Compare results with the SQL procedure.

#### **Test Case 3: Segmentation Logic**
- **Objective**: Validate that customers are segmented correctly based on business rules.
- **Validation**: Check sample records for correct segmentation.

#### **Test Case 4: Performance**
- **Objective**: Ensure that the pipeline can handle large datasets within acceptable time limits.
- **Validation**: Measure processing time for each layer.

#### **Test Case 5: Error Handling**
- **Objective**: Verify that errors are logged and do not cause data corruption.
- **Validation**: Simulate errors (e.g., missing data) and check logs.

---

This implementation plan ensures a robust, scalable, and production-ready migration of the SQL stored procedure to PySpark using the medallion architecture.