# SQL to Medallion

A comprehensive solution for migrating SQL Server stored procedures to a modern medallion architecture on Azure Databricks.

## Overview

SQL to Medallion is a toolkit designed to streamline the migration of legacy SQL Server stored procedures to a modern data lakehouse architecture using the medallion pattern (bronze, silver, gold) on Azure Databricks. It leverages Azure OpenAI to assist with the translation of complex SQL logic to PySpark code.

The solution addresses common challenges in data modernization projects:
- Analyzing and understanding complex legacy SQL code
- Transforming SQL Server-specific syntax to PySpark
- Implementing proper data quality checks
- Maintaining business logic during migration
- Creating testable and maintainable code

## Key Features

- **Automated SQL to PySpark Translation**: Convert SQL Server stored procedures to PySpark code
- **Medallion Architecture Implementation**: Organize data processing into bronze, silver, and gold layers
- **Azure OpenAI Integration**: Leverage AI for complex code translation and analysis
- **Comprehensive Logging**: Track transformation processes and data lineage
- **Modular Design**: Separate modules for extraction, transformation, and loading
- **Error Handling**: Robust error handling and recovery mechanisms
- **Configurable**: Easily adaptable to different project requirements

## Architecture

The solution implements the medallion architecture pattern:

1. **Bronze Layer**: Raw data extracted from SQL Server tables with minimal transformations
2. **Silver Layer**: Cleansed, validated, and transformed data with business-specific logic
3. **Gold Layer**: Analytics-ready data models optimized for specific use cases

## Project Structure

```
sql-to-medallion/
├── config/                     # Configuration files
│   ├── llm_config.json         # LLM configuration settings
│   └── prompts.py              # Prompt templates for LLM
├── data/                       # Sample data and schemas
├── notebooks/                  # Example Databricks notebooks
├── src/                        # Source code
│   ├── bronze/                 # Bronze layer processing
│   │   └── extract.py          # Data extraction functions
│   ├── silver/                 # Silver layer processing
│   │   └── transform.py        # Data transformation functions
│   ├── gold/                   # Gold layer processing
│   │   └── transform.py        # Data aggregation and modeling
│   ├── llm/                    # LLM integration
│   │   └── assistant.py        # OpenAI API interface
│   ├── orchestration/          # Pipeline orchestration
│   └── utils/                  # Utility functions
│       ├── config.py           # Configuration management
│       ├── database.py         # Database connectivity
│       ├── logging.py          # Logging utilities
│       └── spark.py            # Spark session management
└── tests/                      # Unit and integration tests
```

## Usage Examples

### Basic Pipeline Execution

```python
from sql_to_medallion.src.bronze.extract import extract_to_bronze
from sql_to_medallion.src.silver.transform import transform_to_silver
from sql_to_medallion.src.gold.transform import transform_to_gold
from sql_to_medallion.src.utils.spark import get_spark_session

# Initialize Spark session
spark = get_spark_session()

# Extract data from SQL Server to bronze layer
extract_to_bronze(
    spark, 
    source_server="sql-server.database.windows.net",
    source_database="AdventureWorks",
    source_tables=["Sales.Customer", "Sales.SalesOrderHeader"],
    target_path="abfss://bronze@adlsgen2.dfs.core.windows.net/"
)

# Transform bronze data to silver layer
transform_to_silver(
    spark,
    bronze_path="abfss://bronze@adlsgen2.dfs.core.windows.net/",
    silver_path="abfss://silver@adlsgen2.dfs.core.windows.net/",
    tables=["Customer", "SalesOrderHeader"]
)

# Transform silver data to gold layer for analytics
transform_to_gold(
    spark,
    silver_path="abfss://silver@adlsgen2.dfs.core.windows.net/",
    gold_path="abfss://gold@adlsgen2.dfs.core.windows.net/",
    transformation_name="CustomerSalesAnalysis"
)
```

### Using LLM for SQL Translation

```python
from sql_to_medallion.src.llm.assistant import translate_sql_to_pyspark

# Original SQL stored procedure
sql_code = """
CREATE PROCEDURE [dbo].[GetCustomerSummary]
    @CustomerId INT
AS
BEGIN
    SELECT 
        c.CustomerId,
        c.FirstName + ' ' + c.LastName AS CustomerName,
        COUNT(o.OrderId) AS OrderCount,
        SUM(o.OrderTotal) AS TotalSpend
    FROM 
        Customers c
    LEFT JOIN 
        Orders o ON c.CustomerId = o.CustomerId
    WHERE 
        c.CustomerId = @CustomerId
    GROUP BY 
        c.CustomerId, c.FirstName, c.LastName
END
"""

# Translate SQL to PySpark
pyspark_code = translate_sql_to_pyspark(sql_code)

print(pyspark_code)
```

Output:
```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

def get_customer_summary(spark, customer_id):
    """
    Returns a summary of customer information and order statistics.
    
    Args:
        spark: SparkSession
        customer_id: The ID of the customer to summarize
        
    Returns:
        DataFrame containing customer information and order statistics
    """
    # Read from silver layer
    customers = spark.table("silver.customers")
    orders = spark.table("silver.orders")
    
    # Filter and join data
    result = customers.alias("c").join(
        orders.alias("o"),
        on=F.col("c.CustomerId") == F.col("o.CustomerId"),
        how="left"
    ).filter(
        F.col("c.CustomerId") == customer_id
    ).groupBy(
        "c.CustomerId", "c.FirstName", "c.LastName"
    ).agg(
        F.count("o.OrderId").alias("OrderCount"),
        F.sum("o.OrderTotal").alias("TotalSpend")
    ).select(
        F.col("c.CustomerId"),
        F.concat_ws(" ", F.col("c.FirstName"), F.col("c.LastName")).alias("CustomerName"),
        F.col("OrderCount"),
        F.col("TotalSpend")
    )
    
    return result
```

## Getting Started

### Prerequisites

- Python 3.8+
- Azure Databricks workspace
- Azure Data Lake Storage Gen2
- Azure OpenAI API access
- SQL Server with JDBC connectivity

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/data-modernization.git
   cd data-modernization/sql-to-medallion
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up configuration:
   - Update SQL Server connection details in config files
   - Configure Azure OpenAI API details in `config/llm_config.json`
   - Adjust Azure Databricks and ADLS paths as needed

### Deployment

1. Upload the project to your Azure Databricks workspace.

2. Create a Databricks job to orchestrate the pipeline:
   ```json
   {
     "name": "SQL to Medallion Pipeline",
     "tasks": [
       {
         "task_key": "bronze_extraction",
         "notebook_task": {
           "notebook_path": "/Shared/sql-to-medallion/notebooks/bronze_extraction"
         },
         "existing_cluster_id": "your-cluster-id"
       },
       {
         "task_key": "silver_transformation",
         "notebook_task": {
           "notebook_path": "/Shared/sql-to-medallion/notebooks/silver_transformation"
         },
         "existing_cluster_id": "your-cluster-id",
         "depends_on": [
           {
             "task_key": "bronze_extraction"
           }
         ]
       },
       {
         "task_key": "gold_transformation",
         "notebook_task": {
           "notebook_path": "/Shared/sql-to-medallion/notebooks/gold_transformation"
         },
         "existing_cluster_id": "your-cluster-id",
         "depends_on": [
           {
             "task_key": "silver_transformation"
           }
         ]
       }
     ]
   }
   ```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 