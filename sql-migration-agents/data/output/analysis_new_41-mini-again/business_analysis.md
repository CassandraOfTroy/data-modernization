# Business Analysis

Below is a consolidated analysis of the stored procedure from three different perspectives: Business Analyst, Domain (SQL) Expert, and Azure Expert. Each section addresses the specific points requested (business purpose, technical patterns, data sources, business logic, and migration challenges).

────────────────────────────────────────────────────
1. BUSINESS ANALYST PERSPECTIVE
────────────────────────────────────────────────────

A) Overall Business Purpose  
• This procedure is designed to generate customer-centric analytics and segmentations. It consolidates data about customers, orders, returns, and interactions across various tables into a single, analytics-ready format.  
• The key outcome is an enriched view of each customer’s lifetime value, churn risk, loyalty score, and recommended marketing strategy.  
• The results are stored for historical analysis, reporting, and marketing decision-making (like VIP identification, churn prevention, or upsell campaigns).

B) Data Sources and Their Relationships  
• dbo.Customers and dbo.CustomerAddresses: Base customer profile details (e.g., name, email, phone, address, status).  
• dbo.CustomerInteractions: Tracks latest contact dates or interactions to identify recent customer activity.  
• dbo.Orders and dbo.OrderDetails: Used for transaction metrics like total spend, average order value, purchase recency, and product/category information.  
• dbo.Products: Provides product names and categories.  
• dbo.Returns: Used to calculate return rates as part of customer behavior metrics.  
• The procedure merges these sources into temporary staging tables (#CustomerBase, #TransactionSummary, #CustomerMetrics, #CustomerSegments).

C) Key Business Logic and Transformations  
1. Customer Base Extraction: Pulls customers whose records have changed or have recent interactions to ensure analytics only process updated data.  
2. Transaction Summary (Orders → #TransactionSummary): Calculates total orders, total spent, average order value, first/last purchase dates, days since last purchase, top product/category, and return rate.  
3. RFM (Recency-Frequency-Monetary) Scoring: Assigns a recency score (how recently the customer purchased), frequency score (how often they purchase), and monetary score (total spending level).  
4. Additional Metrics:  
   – Churn Probability is derived from recency thresholds (e.g., 180 days = high churn risk).  
   – Next Purchase Propensity uses recency and frequency to guess how likely a customer is to buy again soon.  
   – Loyalty Index factors total orders and return rate into a 0–1 scale.  
   – Customer Health is a categorical label (Excellent, Good, etc.) based on churn probability and loyalty index.  
5. Segmentation:  
   – Value Segment: High, Medium, or Low value based on lifetime value thresholds.  
   – Behavior Segment: Champions, Loyal Customers, Potential Loyalists, At Risk, etc., according to the RFM scores.  
   – Lifecycle Segment: New Customer, Active, At Risk, Churned, etc., based on days since last purchase and churn probability.  
   – Target Group & Marketing Recommendation: Prescriptive actions (VIP treatments, re-engagement campaigns, cross-sells) depending on all above metrics.  
6. Data Persistence: Inserts or merges the updated analytics information into two main targets:  
   – dbo.CustomerProfiles (customer-level details)  
   – dbo.CustomerAnalytics (full analytics data, stored per day)  
   – Also writes a summary row into dbo.CustomerAnalyticsSummary (aggregated stats, e.g., total customers, newly acquired, at-risk count).  

D) Key Business Benefits  
• Identifies high-value customers early to provide premium experiences.  
• Detects churn risk to trigger re-engagement strategies.  
• Suggests segment-specific marketing approaches to optimize retention and cross-selling.  
• Produces daily or periodic summary reporting for management oversight.

────────────────────────────────────────────────────
2. DOMAIN (SQL) EXPERT PERSPECTIVE
────────────────────────────────────────────────────

A) Technical Analysis of SQL Patterns Used  
• Conditional Parameter Defaults: The procedure checks if @DataDate is NULL, then uses the prior day’s date.  
• Temporary Tables (#...): Used extensively to stage intermediate calculations. This is a common ETL pattern in T-SQL to keep logic modular and improve performance.  
• Common Table Expressions (CTEs): WITH clauses (OrderSummary, ProductCategories, etc.) to group logic, clarify transformations, and combine with final SELECT or UPDATE statements.  
• Window Functions (ROW_NUMBER, NTILE): Used for RFM scoring partitions and for identifying top categories/products.  
• MERGE Statement: Updates or inserts records into dbo.CustomerProfiles in batches for efficient “upsert” functionality.  
• Batching Logic: The procedure uses loops and a batch size (@BatchSize) to handle large data volumes.  
• Error Handling with TRY/CATCH: Rolls back transactions and logs errors to maintain data consistency.  
• Clean Up & Housekeeping: Temporary tables are dropped at the end, and older analytics records are removed according to the retention period setting.

B) Observed SQL Best Practices  
• Clustered indexes on staged tables to optimize the join and aggregation performance.  
• Summaries stored in separate tables (e.g., CustomerAnalytics, CustomerAnalyticsSummary).  
• Use of parameters to control churn thresholds, high-value thresholds, and retention days, making the solution more flexible and maintainable.

────────────────────────────────────────────────────
3. AZURE EXPERT PERSPECTIVE (MIGRATION TO PYSPARK)
────────────────────────────────────────────────────

A) Potential Challenges for Migration to PySpark  
1) Temporary Tables and Iterative Batching:  
   – PySpark might use DataFrames and distributed computations instead of many temporary tables. Replacing T-SQL temporary table staging with Spark DataFrame transformations requires reorganizing the workflow into pipelines.  
2) Window Functions (NTILE, ROW_NUMBER) and RFM Calculations:  
   – Spark SQL supports window functions, but rewriting the partition logic (NTILE for 5-part recency/frequency splits) may need adaptation.  
3) MERGE Equivalent:  
   – Merging changes into a dimension table (CustomerProfiles) is not natively supported in older Spark versions. In newer versions or in Delta Lake on Microsoft Fabric, MERGE is available, but you must confirm your environment supports it. Otherwise, implement an “upsert” approach via separate insert/update steps.  
4) Transaction and Error Handling:  
   – Spark does not use transactions in the same way as SQL. You’ll need to handle partial failures carefully, potentially re-designing error-handling and rollback logic (often with idempotent or snapshot approaches in Spark).  
5) Parameter-Driven Logic for Churn & Thresholds:  
   – Spark jobs can read configurations from external files or parameter tables, but you’ll need a new orchestration flow (e.g., with Azure Data Factory or a Spark driver script) to replicate how T-SQL parameters are set and used.  

B) Additional Considerations for Microsoft Fabric  
• Fabric Lakehouse or Warehouse may store data in Delta format, enabling ACID transactions and MERGE operations, lowering some migration friction.  
• PySpark notebooks in Fabric can orchestrate each transformation step, but likely you will reorganize the procedure logic into a series of well-defined transformations rather than one single script.  
• For incremental runs, verifying last modified date or other triggers will be essential to handle partial loads in Spark.  

────────────────────────────────────────────────────

SUMMARY

• From a business perspective, this stored procedure calculates a complete 360° view of customers (RFM, churn probability, segmentations, marketing suggestions) to drive retention and growth strategies.  
• Technically, it leverages SQL staging tables, window functions, merges, and robust error handling to produce daily analytics and summarize key performance metrics.  
• When migrating to PySpark or Microsoft Fabric, focus on shifting temporary-table workflows to Spark DataFrames or Delta tables, re-implementing merges, and adjusting transaction/error handling to accommodate distributed data processing.