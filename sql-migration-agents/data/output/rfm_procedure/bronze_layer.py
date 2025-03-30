# BRONZE LAYER START
from pyspark.sql import SparkSession

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
# BRONZE LAYER END