# BRONZE LAYER START: Raw Data Ingestion
from pyspark.sql import SparkSession

# Load raw data from SQL tables into Delta Lake
def load_table(table_name):
    return spark.read.format("jdbc").options(
        url="jdbc:sqlserver://<server>;database=<db>",
        dbtable=table_name,
        user="<username>",
        password="<password>"
    ).load()

# Load raw data
customers = load_table("dbo.Customers")
customer_addresses = load_table("dbo.CustomerAddresses")
customer_interactions = load_table("dbo.CustomerInteractions")
orders = load_table("dbo.Orders")
order_details = load_table("dbo.OrderDetails")
products = load_table("dbo.Products")
returns = load_table("dbo.Returns")

# Save raw data to Bronze Layer
def save_to_bronze(df, path):
    df.write.format("delta").mode("overwrite").save(path)

save_to_bronze(customers, "/bronze/customers")
save_to_bronze(customer_addresses, "/bronze/customer_addresses")
save_to_bronze(customer_interactions, "/bronze/customer_interactions")
save_to_bronze(orders, "/bronze/orders")
save_to_bronze(order_details, "/bronze/order_details")
save_to_bronze(products, "/bronze/products")
save_to_bronze(returns, "/bronze/returns")
# BRONZE LAYER END