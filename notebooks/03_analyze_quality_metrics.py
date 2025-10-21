# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Metrics Analysis
# MAGIC 
# MAGIC This notebook provides queries and visualizations for analyzing DQX data quality metrics.
# MAGIC Use this notebook to monitor data quality trends and identify problematic data patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get parameters or use defaults
catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "main"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "dqx_demo"

# Create widgets for parameters
dbutils.widgets.text("catalog", catalog, "Catalog")
dbutils.widgets.text("schema", schema, "Schema")

target_schema = f"{catalog}.{schema}"
print(f"Analyzing quality metrics for: {target_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Quality Summary
# MAGIC 
# MAGIC View the latest data quality pass/fail rates across all tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   bronze_records,
# MAGIC   silver_records,
# MAGIC   quarantine_records,
# MAGIC   quality_pass_rate,
# MAGIC   quality_fail_rate,
# MAGIC   summary_timestamp
# MAGIC FROM ${catalog}.${schema}.dqx_quality_summary
# MAGIC ORDER BY summary_timestamp DESC
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Trends Over Time

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   DATE(summary_timestamp) as date,
# MAGIC   table_name,
# MAGIC   AVG(quality_pass_rate) as avg_pass_rate,
# MAGIC   AVG(quality_fail_rate) as avg_fail_rate,
# MAGIC   SUM(bronze_records) as total_records,
# MAGIC   SUM(quarantine_records) as total_quarantined
# MAGIC FROM ${catalog}.${schema}.dqx_quality_summary
# MAGIC GROUP BY DATE(summary_timestamp), table_name
# MAGIC ORDER BY date DESC, table_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantined Records Analysis
# MAGIC 
# MAGIC Examine records that failed quality checks for each table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer Data Issues

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   phone,
# MAGIC   country,
# MAGIC   registration_date,
# MAGIC   quarantine_timestamp,
# MAGIC   quarantine_reason,
# MAGIC   CASE 
# MAGIC     WHEN customer_name IS NULL THEN 'Missing customer name'
# MAGIC     WHEN email IS NULL OR email = '' THEN 'Missing or blank email'
# MAGIC     WHEN email NOT LIKE '%@%.%' THEN 'Invalid email format'
# MAGIC     WHEN phone IS NULL THEN 'Missing phone'
# MAGIC     WHEN country IS NULL THEN 'Missing country'
# MAGIC     WHEN registration_date IS NULL THEN 'Missing registration date'
# MAGIC     ELSE 'Other issue'
# MAGIC   END as issue_type
# MAGIC FROM ${catalog}.${schema}.customers_quarantine
# MAGIC ORDER BY quarantine_timestamp DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product Data Issues

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   product_name,
# MAGIC   category,
# MAGIC   unit_price,
# MAGIC   stock_quantity,
# MAGIC   quarantine_timestamp,
# MAGIC   quarantine_reason,
# MAGIC   CASE 
# MAGIC     WHEN product_name IS NULL OR product_name = '' THEN 'Missing product name'
# MAGIC     WHEN category NOT IN ('Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Toys') THEN 'Invalid category'
# MAGIC     WHEN unit_price IS NULL OR unit_price <= 0 THEN 'Invalid unit price'
# MAGIC     WHEN stock_quantity IS NULL OR stock_quantity < 0 THEN 'Invalid stock quantity'
# MAGIC     ELSE 'Other issue'
# MAGIC   END as issue_type
# MAGIC FROM ${catalog}.${schema}.products_quarantine
# MAGIC ORDER BY quarantine_timestamp DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order Data Issues

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   customer_id,
# MAGIC   product_id,
# MAGIC   order_date,
# MAGIC   quantity,
# MAGIC   order_amount,
# MAGIC   status,
# MAGIC   quarantine_timestamp,
# MAGIC   quarantine_reason,
# MAGIC   CASE 
# MAGIC     WHEN customer_id IS NULL THEN 'Missing customer ID'
# MAGIC     WHEN product_id IS NULL THEN 'Missing product ID'
# MAGIC     WHEN order_date IS NULL THEN 'Missing order date'
# MAGIC     WHEN quantity IS NULL OR quantity <= 0 THEN 'Invalid quantity'
# MAGIC     WHEN order_amount IS NULL OR order_amount <= 0 THEN 'Invalid order amount'
# MAGIC     WHEN status IS NULL OR status = '' THEN 'Missing or blank status'
# MAGIC     WHEN status NOT IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled') THEN 'Invalid status'
# MAGIC     ELSE 'Other issue'
# MAGIC   END as issue_type
# MAGIC FROM ${catalog}.${schema}.orders_quarantine
# MAGIC ORDER BY quarantine_timestamp DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Issue Type Distribution

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   'customers' as table_name,
# MAGIC   CASE 
# MAGIC     WHEN customer_name IS NULL THEN 'Missing customer name'
# MAGIC     WHEN email IS NULL OR email = '' THEN 'Missing or blank email'
# MAGIC     WHEN email NOT LIKE '%@%.%' THEN 'Invalid email format'
# MAGIC     WHEN phone IS NULL THEN 'Missing phone'
# MAGIC     WHEN country IS NULL THEN 'Missing country'
# MAGIC     WHEN registration_date IS NULL THEN 'Missing registration date'
# MAGIC     ELSE 'Other issue'
# MAGIC   END as issue_type,
# MAGIC   COUNT(*) as issue_count
# MAGIC FROM ${catalog}.${schema}.customers_quarantine
# MAGIC GROUP BY issue_type
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'products' as table_name,
# MAGIC   CASE 
# MAGIC     WHEN product_name IS NULL OR product_name = '' THEN 'Missing product name'
# MAGIC     WHEN category NOT IN ('Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Toys') THEN 'Invalid category'
# MAGIC     WHEN unit_price IS NULL OR unit_price <= 0 THEN 'Invalid unit price'
# MAGIC     WHEN stock_quantity IS NULL OR stock_quantity < 0 THEN 'Invalid stock quantity'
# MAGIC     ELSE 'Other issue'
# MAGIC   END as issue_type,
# MAGIC   COUNT(*) as issue_count
# MAGIC FROM ${catalog}.${schema}.products_quarantine
# MAGIC GROUP BY issue_type
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'orders' as table_name,
# MAGIC   CASE 
# MAGIC     WHEN customer_id IS NULL THEN 'Missing customer ID'
# MAGIC     WHEN product_id IS NULL THEN 'Missing product ID'
# MAGIC     WHEN order_date IS NULL THEN 'Missing order date'
# MAGIC     WHEN quantity IS NULL OR quantity <= 0 THEN 'Invalid quantity'
# MAGIC     WHEN order_amount IS NULL OR order_amount <= 0 THEN 'Invalid order amount'
# MAGIC     WHEN status IS NULL OR status = '' THEN 'Missing or blank status'
# MAGIC     WHEN status NOT IN ('pending', 'processing', 'shipped', 'delivered', 'cancelled') THEN 'Invalid status'
# MAGIC     ELSE 'Other issue'
# MAGIC   END as issue_type,
# MAGIC   COUNT(*) as issue_count
# MAGIC FROM ${catalog}.${schema}.orders_quarantine
# MAGIC GROUP BY issue_type
# MAGIC
# MAGIC ORDER BY table_name, issue_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Table Quality
# MAGIC 
# MAGIC Verify the quality of the enriched gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_orders,
# MAGIC   COUNT(DISTINCT customer_name) as unique_customers,
# MAGIC   COUNT(DISTINCT product_name) as unique_products,
# MAGIC   SUM(order_amount) as total_revenue,
# MAGIC   AVG(order_amount) as avg_order_value,
# MAGIC   MIN(order_date) as earliest_order,
# MAGIC   MAX(order_date) as latest_order
# MAGIC FROM ${catalog}.${schema}.orders_gold

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   status,
# MAGIC   COUNT(*) as order_count,
# MAGIC   SUM(order_amount) as total_amount,
# MAGIC   AVG(order_amount) as avg_amount
# MAGIC FROM ${catalog}.${schema}.orders_gold
# MAGIC GROUP BY status
# MAGIC ORDER BY order_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   product_category,
# MAGIC   COUNT(*) as order_count,
# MAGIC   SUM(order_amount) as total_revenue,
# MAGIC   AVG(order_amount) as avg_order_value
# MAGIC FROM ${catalog}.${schema}.orders_gold
# MAGIC GROUP BY product_category
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Recommendations
# MAGIC 
# MAGIC Based on the quarantine analysis, identify areas for improvement.

# COMMAND ----------

# Get quarantine counts
quarantined_customers = spark.table(f"{target_schema}.customers_quarantine").count()
quarantined_products = spark.table(f"{target_schema}.products_quarantine").count()
quarantined_orders = spark.table(f"{target_schema}.orders_quarantine").count()

# Get total counts
total_customers = spark.table(f"{target_schema}.customers_bronze").count()
total_products = spark.table(f"{target_schema}.products_bronze").count()
total_orders = spark.table(f"{target_schema}.orders_bronze").count()

# Calculate percentages
percent_quarantined_customers = (quarantined_customers / total_customers * 100) if total_customers > 0 else 0
percent_quarantined_products = (quarantined_products / total_products * 100) if total_products > 0 else 0
percent_quarantined_orders = (quarantined_orders / total_orders * 100) if total_orders > 0 else 0

print("=" * 80)
print("DATA QUALITY SUMMARY")
print("=" * 80)
print("Customers:")
print(f"  - Total: {total_customers:,}")
print(f"  - Quarantined: {quarantined_customers:,} ({percent_quarantined_customers:.2f}%)")
print(f"  - Valid: {total_customers - quarantined_customers:,}")

print("Products:")
print(f"  - Total: {total_products:,}")
print(f"  - Quarantined: {quarantined_products:,} ({percent_quarantined_products:.2f}%)")
print(f"  - Valid: {total_products - quarantined_products:,}")

print("Orders:")
print(f"  - Total: {total_orders:,}")
print(f"  - Quarantined: {quarantined_orders:,} ({percent_quarantined_orders:.2f}%)")
print(f"  - Valid: {total_orders - quarantined_orders:,}")

print("\n" + "=" * 80)
print("RECOMMENDATIONS")
print("=" * 80)

if percent_quarantined_customers > 5:
    print("Customer data quality is below target (>5% failure rate)")
    print("   -> Review data ingestion processes")
    print("   -> Consider implementing upstream validations")
    
if percent_quarantined_products > 5:
    print("Product data quality is below target (>5% failure rate)")
    print("   -> Validate product catalog at source")
    print("   -> Implement price validation rules")
    
if percent_quarantined_orders > 5:
    print("Order data quality is below target (>5% failure rate)")
    print("   -> Review order entry system validations")
    print("   -> Consider implementing pre-processing checks")

if max(percent_quarantined_customers, percent_quarantined_products, percent_quarantined_orders) <= 5:
    print("Data quality is meeting targets (<5% failure rate across all tables)")
    print("   -> Continue monitoring and maintain current standards")

print("\n" + "=" * 80)
