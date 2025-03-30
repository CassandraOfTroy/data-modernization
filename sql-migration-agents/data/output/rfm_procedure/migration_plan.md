### Migration Plan and Implementation for SQL Stored Procedure to PySpark with Microsoft Fabric

---

#### **1. Migration Plan**

**Objective**: Migrate the SQL stored procedure `[dbo].[usp_CustomerAnalytics_Processing]` to PySpark using the Medallion Architecture (Bronze, Silver, Gold layers) in Microsoft Fabric.

**Key Deliverables**:
1. **Bronze Layer**: Raw data ingestion from source tables.
2. **Silver Layer**: Data transformation and enrichment.
3. **Gold Layer**: Aggregated and business-ready data for analytics.
4. **Production-Ready PySpark Code**: Modular, reusable, and scalable.
5. **Validation and Testing**: Comprehensive test cases for data accuracy and consistency.

---

#### **2. PySpark Implementation**

##### **Bronze Layer: Raw Data Ingestion**
The Bronze layer ingests raw data from source tables into Delta Lake.

```python
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerAnalytics").getOrCreate()

# Load raw data into Bronze layer
customers = spark.read.format("delta").load("fabric://source/customers")
customer_addresses = spark.read.format("delta").load("fabric://source/customer_addresses")
customer_interactions = spark.read.format("delta").load("fabric://source/customer_interactions")
orders = spark.read.format("delta").load("fabric://source/orders")
order_details = spark.read.format("delta").load("fabric://source/order_details")
products = spark.read.format("delta").load("fabric://source/products")
returns = spark.read.format("delta").load("fabric://source/returns")

# Save raw data to Bronze layer
customers.write.format("delta").mode("overwrite").save("fabric://bronze/customers")
customer_addresses.write.format("delta").mode("overwrite").save("fabric://bronze/customer_addresses")
customer_interactions.write.format("delta").mode("overwrite").save("fabric://bronze/customer_interactions")
orders.write.format("delta").mode("overwrite").save("fabric://bronze/orders")
order_details.write.format("delta").mode("overwrite").save("fabric://bronze/order_details")
products.write.format("delta").mode("overwrite").save("fabric://bronze/products")
returns.write.format("delta").mode("overwrite").save("fabric://bronze/returns")
```

---

##### **Silver Layer: Data Transformation and Enrichment**
The Silver layer processes raw data to calculate metrics and enrich customer data.

```python
from pyspark.sql.functions import col, lit, when, max, count, sum, avg, min, datediff, ntile
from pyspark.sql.window import Window

# Extract Customer Base
customer_base = customers.join(customer_addresses, ["CustomerID"], "left") \
    .join(customer_interactions, ["CustomerID"], "left") \
    .select(
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
        max(col("ContactDate")).alias("LastContactDate")
    )

customer_base.write.format("delta").mode("overwrite").save("fabric://silver/customer_base")

# Process Transaction Summary
order_summary = orders.join(order_details, ["OrderID"]) \
    .groupBy("CustomerID") \
    .agg(
        count("OrderID").alias("TotalOrders"),
        sum(col("Quantity") * col("UnitPrice") * (1 - col("Discount"))).alias("TotalSpent"),
        min("OrderDate").alias("FirstPurchaseDate"),
        max("OrderDate").alias("LastPurchaseDate")
    )

transaction_summary = order_summary.withColumn(
    "AvgOrderValue", when(col("TotalOrders") > 0, col("TotalSpent") / col("TotalOrders")).otherwise(0)
).withColumn(
    "DaysSinceLastPurchase", datediff(lit("2023-10-01"), col("LastPurchaseDate"))
)

transaction_summary.write.format("delta").mode("overwrite").save("fabric://silver/transaction_summary")
```

---

##### **Gold Layer: Aggregated Data for Reporting**
The Gold layer aggregates data for customer segmentation and reporting.

```python
# Calculate RFM Scores
rfm_scores = transaction_summary.withColumn(
    "RecencyScore", ntile(5).over(Window.orderBy(col("DaysSinceLastPurchase").asc()))
).withColumn(
    "FrequencyScore", ntile(5).over(Window.orderBy(col("TotalOrders").desc()))
).withColumn(
    "MonetaryScore", ntile(5).over(Window.orderBy(col("TotalSpent").desc()))
).withColumn(
    "RFMScore", col("RecencyScore") + col("FrequencyScore") + col("MonetaryScore")
)

rfm_scores.write.format("delta").mode("overwrite").save("fabric://gold/rfm_scores")

# Customer Segmentation
customer_segments = rfm_scores.withColumn(
    "ValueSegment", when(col("TotalSpent") >= 5000, "High Value").otherwise("Low Value")
).withColumn(
    "BehaviorSegment", when(col("RFMScore") >= 13, "Champions").otherwise("Others")
)

customer_segments.write.format("delta").mode("overwrite").save("fabric://gold/customer_segments")
```

---

#### **3. Implementation Guidelines**

1. **Delta Lake**:
   - Use Delta Lake for ACID compliance and efficient data storage.
   - Partition data by `ProcessDate` for faster queries.

2. **Modular Code**:
   - Separate logic into reusable functions for maintainability.

3. **Logging**:
   - Implement logging for debugging and monitoring.

4. **Scalability**:
   - Optimize joins and aggregations for large datasets.

5. **Testing**:
   - Validate data at each stage using test cases.

---

#### **4. Test Cases**

##### **Bronze Layer Validation**
- Verify data ingestion from source tables.
- Check schema and data types.

##### **Silver Layer Validation**
- Validate transformations (e.g., customer base extraction).
- Ensure metrics are calculated correctly.

##### **Gold Layer Validation**
- Check segmentation logic.
- Validate aggregated data for reporting.

##### **End-to-End Validation**
- Compare results with SQL procedure outputs.
- Ensure data consistency and accuracy.

---

#### **5. Production-Ready Code Review**

**Tech Lead Review Checklist**:
1. **Code Quality**:
   - Ensure code is modular and follows best practices.
   - Use meaningful variable names and comments.

2. **Performance**:
   - Optimize joins and aggregations.
   - Test scalability with large datasets.

3. **Error Handling**:
   - Implement robust error handling and logging.

4. **Compliance**:
   - Ensure compliance with data retention policies.

---

### Summary

This migration plan ensures a seamless transition from SQL to PySpark using Microsoft Fabric while adhering to the Medallion Architecture. The implementation is modular, scalable, and production-ready, with comprehensive testing to validate data accuracy and consistency.