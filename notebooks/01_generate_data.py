# Databricks notebook source
# MAGIC %md
# MAGIC # Data Generation Notebook
# MAGIC 
# MAGIC This notebook generates a multi-table sales order dataset using Databricks Labs' data generator.
# MAGIC Data quality issues are randomly injected to demonstrate DQX capabilities.

# COMMAND ----------

# MAGIC %pip install dbldatagen
# MAGIC %restart_python

# COMMAND ----------

import random
import dbldatagen as dg

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get parameters or use defaults
catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "main"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "dqx_demo"
volume = dbutils.widgets.get("volume") if dbutils.widgets.get("volume") else "raw_data"
num_rows = int(dbutils.widgets.get("num_rows")) if dbutils.widgets.get("num_rows") else 10000

# Create widgets for parameters
dbutils.widgets.text("catalog", catalog, "Catalog")
dbutils.widgets.text("schema", schema, "Schema")
dbutils.widgets.text("volume", volume, "Volume")
dbutils.widgets.text("num_rows", str(num_rows), "Number of Rows")

# COMMAND ----------

# Ensure catalog, schema, and volume exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

volume_path = f"/Volumes/{catalog}/{schema}/{volume}"
print(f"Data will be written to: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customers Table

# COMMAND ----------

# Generate customers
customers_spec = (
    dg.DataGenerator(spark, name="customers", rows=num_rows // 10, partitions=4)
    .withIdOutput()
    .withColumn("customer_name", "string", template=r"\w \w|\w \w \w")
    .withColumn("email", "string", template=r"\w.\w@\w.com")
    .withColumn("phone", "string", template=r"\d\d\d-\d\d\d-\d\d\d\d")
    .withColumn("country", "string", values=["USA", "Canada", "UK", "Germany", "France", "Japan", "Australia"])
    .withColumn("registration_date", "date", begin="2020-01-01", end="2024-12-31")
)

customers_df = customers_spec.build()

# Inject data quality issues into customers (10% of rows)
has_issue = F.rand() < 0.1
customers_with_issues_df = customers_df.select(
    "id",
    F.when(has_issue & (F.rand() < 0.5), F.lit(None)).otherwise(F.col("customer_name")).alias("customer_name"),
    F.when(has_issue & (F.rand() < 0.3), F.lit("")).otherwise(F.col("email")).alias("email"),
    "phone",
    "country",
    "registration_date"
)

# Write to CSV
customers_path = f"{volume_path}/customers"
customers_with_issues_df.write.mode("overwrite").option("header", "true").csv(customers_path)
print(f"Generated {customers_with_issues_df.count()} customers at {customers_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Products Table

# COMMAND ----------

# Generate products
products_spec = (
    dg.DataGenerator(spark, name="products", rows=num_rows // 100, partitions=4)
    .withIdOutput()
    .withColumn("product_name", "string", template=r"\w \w Product")
    .withColumn("category", "string", values=["Electronics", "Clothing", "Home", "Sports", "Books", "Toys"])
    .withColumn("unit_price", "decimal(10,2)", minValue=5.0, maxValue=500.0)
    .withColumn("stock_quantity", "integer", minValue=0, maxValue=1000)
)

products_df = products_spec.build()

# Inject data quality issues into products (10% of rows)
has_issue = F.rand() < 0.1
products_with_issues_df = products_df.select(
    "id",
    "product_name",
    "category",
    F.when(has_issue & (F.rand() < 0.5), F.lit(-1.0)).otherwise(F.col("unit_price")).alias("unit_price"),
    F.when(has_issue & (F.rand() < 0.5), F.lit(-1)).otherwise(F.col("stock_quantity")).alias("stock_quantity")
)

# Write to CSV
products_path = f"{volume_path}/products"
products_with_issues_df.write.mode("overwrite").option("header", "true").csv(products_path)
print(f"Generated {products_with_issues_df.count()} products at {products_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Orders Table

# COMMAND ----------

# Get actual customer and product IDs
customer_ids = [row.id for row in customers_df.select("id").collect()]
product_ids = [row.id for row in products_df.select("id").collect()]

# Generate orders
num_rows = num_rows if random.random() < 0.9 else round(num_rows * 0.01)
orders_spec = (
    dg.DataGenerator(spark, name="orders", rows=num_rows, partitions=4)
    .withColumn("id", "string", expr="uuid()")
    .withColumn("customer_id", "integer", values=customer_ids)
    .withColumn("product_id", "integer", values=product_ids)
    .withColumn("order_date", "timestamp", begin="2024-01-01 00:00:00", end="2024-12-31 23:59:59")
    .withColumn("quantity", "integer", minValue=1, maxValue=10)
    .withColumn("order_amount", "decimal(10,2)", minValue=10.0, maxValue=5000.0)
    .withColumn("status", "string", values=["pending", "processing", "shipped", "delivered", "cancelled"])
)

orders_df = orders_spec.build()

# Inject data quality issues into orders (15% of rows)
has_issue = F.rand() < 0.15
orders_with_issues_df = orders_df.select(
    "id",
    F.when(has_issue & (F.rand() < 0.2), F.lit(None)).otherwise(F.col("customer_id")).alias("customer_id"),
    "product_id",
    "order_date",
    F.when(has_issue & (F.rand() < 0.3), F.lit(0)).otherwise(F.col("quantity")).alias("quantity"),
    F.when(has_issue & (F.rand() < 0.4), F.lit(-100.0)).otherwise(F.col("order_amount")).alias("order_amount"),
    F.when(has_issue & (F.rand() < 0.1), F.lit("")).otherwise(F.col("status")).alias("status")
)

# Write to CSV
orders_path = f"{volume_path}/orders"
orders_with_issues_df.write.mode("append").option("header", "true").csv(orders_path)
print(f"Generated {orders_with_issues_df.count()} orders at {orders_path}")
