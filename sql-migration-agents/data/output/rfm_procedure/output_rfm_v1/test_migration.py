  bronze_customers = spark.read.format("delta").load("/mnt/delta/bronze/customers")
  assert bronze_customers.count() == expected_row_count_customers

  transaction_summary = spark.read.format("delta").load("/mnt/delta/silver/transaction_summary")
  sample_customer_metrics = transaction_summary.filter(col("CustomerID") == sample_customer_id).collect()
  assert sample_customer_metrics[0]["TotalOrders"] == expected_total_orders
  assert sample_customer_metrics[0]["TotalSpent"] == expected_total_spent

  customer_segments = spark.read.format("delta").load("/mnt/delta/gold/customer_segments")
  sample_segment = customer_segments.filter(col("CustomerID") == sample_customer_id).collect()
  assert sample_segment[0]["ValueSegment"] == "High Value"
  assert sample_segment[0]["BehaviorSegment"] == "Champions"
  assert sample_segment[0]["LifecycleSegment"] == "Active"

  gold_data = spark.read.format("delta").load("/mnt/delta/gold/customer_segments")
  assert gold_data.filter(col("ProcessDate") < retention_cutoff_date).count() == 0

  gold_data = spark.read.format("delta").load("/mnt/delta/gold/customer_segments")
  assert gold_data.count() == expected_gold_layer_row_count

  transaction_summary = spark.read.format("delta").load("/mnt/delta/silver/transaction_summary")
  assert transaction_summary.filter(col("TotalOrders") == 0).count() == expected_no_order_customers