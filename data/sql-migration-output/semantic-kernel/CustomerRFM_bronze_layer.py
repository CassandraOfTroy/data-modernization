```python
# BRONZE LAYER: Ingesting Raw Data

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, when

# Assuming Spark session is already created

# Load Customers data
customers_df = spark.read.format("delta").load("path_to_customers")

# Load Customer Addresses data
customer_addresses_df = spark.read.format("delta").load("path_to_customer_addresses")

# Load Customer Interactions data
customer_interactions_df = spark.read.format("delta").load("path_to_customer_interactions")

# Create Bronze Layer DataFrame
customers_bronze = customers_df.alias("c")
customer_addresses_bronze = customer_addresses_df.alias("a")
customer_interactions_bronze = customer_interactions_df.alias("i")

# Join to create a base customer DataFrame
customer_base_df = customers_bronze.join(customer_addresses_bronze,
    (col("c.CustomerID") == col("a.CustomerID")) & (col("a.AddressType") == "Primary"), "left")
    .join(customer_interactions_bronze, col("c.CustomerID" == col("i.CustomerID"), "left")
    .select(
        col("c.CustomerID"),
        (col("c.FirstName") + " " + col("c.LastName")).alias("CustomerName"),
        col("c.Email"),
        col("c.Phone"),
        col("a.StreetAddress"),
        col("a.PostalCode"),
        col("a.City"),
        col("a.State"),
        col("a.Country"),
        col("c.CustomerType"),
        col("c.AccountManagerID"),
        col("c.CreatedDate"),
        col("c.ModifiedDate"),
        when(col("c.Status") == "Active", 1).otherwise(0).alias("IsActive"),
        max(col("i.ContactDate")).alias("LastContactDate")
    )
    .groupBy(
        "CustomerID", "CustomerName", "Email", "Phone", "StreetAddress", "PostalCode", "City", "State", "Country", "CustomerType", "AccountManagerID", "CreatedDate", "ModifiedDate", "Status"
    )
```