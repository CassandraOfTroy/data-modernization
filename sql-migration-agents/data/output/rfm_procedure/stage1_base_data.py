# STAGE 1: BASE DATA START
from pyspark.sql.functions import col, lit, when, max, concat_ws

# Load Bronze Layer data
customers = spark.read.format("delta").load("/bronze/customers")
customer_addresses = spark.read.format("delta").load("/bronze/customer_addresses")
customer_interactions = spark.read.format("delta").load("/bronze/customer_interactions")

# Create Customer Base
customer_base = customers.join(
    customer_addresses.filter(col("AddressType") == "Primary"),
    "CustomerID",
    "left"
).join(
    customer_interactions.groupBy("CustomerID").agg(max("ContactDate").alias("LastContactDate")),
    "CustomerID",
    "left"
).select(
    col("CustomerID"),
    concat_ws(" ", col("FirstName"), col("LastName")).alias("CustomerName"),
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

# Save Customer Base to Silver Layer
customer_base.write.format("delta").mode("overwrite").save("/silver/customer_base")
# STAGE 1: BASE DATA END