```python
# BRONZE LAYER: DATA INGESTION START

# Load raw data from source tables into DataFrames
customers_df = spark.read.table("dbo.Customers")
customer_addresses_df = spark.read.table("dbo.CustomerAddresses")
customer_interactions_df = spark.read.table("dbo.CustomerInteractions")
orders_df = spark.read.table("dbo.Orders")
order_details_df = spark.read.table("dbo.OrderDetails")
products_df = spark.read.table("dbo.Products")
returns_df = spark.read.table("dbo.Returns")

# BRONZE LAYER: DATA INGESTION END
```