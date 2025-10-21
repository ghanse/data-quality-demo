# Databricks notebook source
# MAGIC %md
# MAGIC # DQX Data Quality Pipeline (PySpark)
# MAGIC 
# MAGIC This notebook implements a Lakeflow Declarative pipeline with streaming tables using PySpark.
# MAGIC It ingests CSV data using Auto Loader and applies DQX data quality checks using metadata-driven rules.
# MAGIC 
# MAGIC **DQX Documentation**: https://databrickslabs.github.io/dqx/

# COMMAND ----------

# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import dlt
import yaml

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *

from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Get configuration from pipeline settings
volume_path = spark.conf.get("volume_path")
checkpoint_location = spark.conf.get("checkpoint_location")
rules_location = spark.conf.get("rules_location", "/Workspace/.bundle/config")

print(f"Volume path: {volume_path}")
print(f"Checkpoint location: {checkpoint_location}")
print(f"Rules location: {rules_location}")

# Initialize DQX engine once at the module level
workspace_client = WorkspaceClient()
dq_engine = DQEngine(workspace_client)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer - Auto Loader Ingestion
# MAGIC 
# MAGIC Stream CSV files from Unity Catalog volumes using Auto Loader.

# COMMAND ----------

@dlt.table(
    name="customers_bronze",
    comment="Raw customer data ingested from CSV files using Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "id"
    }
)
def customers_bronze() -> DataFrame:
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{checkpoint_location}/customers")
        .load(f"{volume_path}/customers")
        .select(
            F.col("id").cast("int").alias("id"),
            F.col("customer_name"),
            F.col("email"),
            F.col("phone"),
            F.col("country"),
            F.col("registration_date").cast("date").alias("registration_date"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.input_file_name().alias("source_file")
        )
    )

# COMMAND ----------

@dlt.table(
    name="products_bronze",
    comment="Raw product data ingested from CSV files using Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "id"
    }
)
def products_bronze() -> DataFrame:
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{checkpoint_location}/products")
        .load(f"{volume_path}/products")
        .select(
            F.col("id").cast("int").alias("id"),
            F.col("product_name"),
            F.col("category"),
            F.col("unit_price").cast("decimal(10,2)").alias("unit_price"),
            F.col("stock_quantity").cast("int").alias("stock_quantity"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.input_file_name().alias("source_file")
        )
    )

# COMMAND ----------

@dlt.table(
    name="orders_bronze",
    comment="Raw order data ingested from CSV files using Auto Loader",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "id"
    }
)
def orders_bronze() -> DataFrame:
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("cloudFiles.schemaLocation", f"{checkpoint_location}/orders")
        .load(f"{volume_path}/orders")
        .select(
            F.col("id").cast("int").alias("id"),
            F.col("customer_id").cast("int").alias("customer_id"),
            F.col("product_id").cast("int").alias("product_id"),
            F.col("order_date").cast("timestamp").alias("order_date"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("order_amount").cast("decimal(10,2)").alias("order_amount"),
            F.col("status"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.input_file_name().alias("source_file")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer - Apply DQX Checks and Filter Valid Records
# MAGIC 
# MAGIC Apply DQX quality checks at the silver layer and use DLT expectations to route data.
# MAGIC Valid records (no errors) go to silver, invalid records go to quarantine.

# COMMAND ----------

@dlt.table(
    name="customers_silver",
    comment="Validated customer data - records with no DQX errors",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "id"
    }
)
@dlt.expect("no_dqx_warnings", "_warnings IS NULL")
@dlt.expect_or_drop("no_dqx_errors", "_errors IS NULL")
def customers_silver(engine: DQEngine = dq_engine) -> DataFrame:
    # Read from bronze layer
    bronze_df = dlt.read_stream("customers_bronze")
    
    # Load rules from YAML
    yaml_file = f"{rules_location}/customers_rules.yaml"
    checks = yaml.safe_load(yaml_file)
    
    # Apply DQX checks using the shared DQEngine
    return engine.apply_checks_by_metadata(bronze_df, checks)

# COMMAND ----------

@dlt.table(
    name="products_silver",
    comment="Validated product data - records with no DQX errors",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "id"
    }
)
@dlt.expect("no_dqx_warnings", "_warnings IS NULL")
@dlt.expect_or_drop("no_dqx_errors", "_errors IS NULL")
def products_silver(engine: DQEngine = dq_engine) -> DataFrame:
    # Read from bronze layer
    bronze_df = dlt.read_stream("products_bronze")
    
    # Load rules from YAML
    yaml_file = f"{rules_location}/products_rules.yaml"
    checks = yaml.safe_load(yaml_file)
    
    # Apply DQX checks using the shared DQEngine
    return dq_engine.apply_checks_by_metadata(bronze_df, checks)

# COMMAND ----------

@dlt.table(
    name="orders_silver",
    comment="Validated order data - records with no DQX errors",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "id"
    }
)
@dlt.expect("no_dqx_warnings", "_warnings IS NULL")
@dlt.expect_or_drop("no_dqx_errors", "_errors IS NULL")
def orders_silver(engine: DQEngine = dq_engine) -> DataFrame:
    # Read from bronze layer
    bronze_df = dlt.read_stream("orders_bronze")
    
    # Load rules from YAML
    yaml_file = f"{rules_location}/orders_rules.yaml"
    checks = yaml.safe_load(yaml_file)
    
    # Apply DQX checks using the shared DQEngine
    return dq_engine.apply_checks_by_metadata(bronze_df, checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine Tables - Records with DQX Errors
# MAGIC 
# MAGIC Capture records that fail DQX quality checks for analysis and remediation.
# MAGIC Uses the same DQX checks as silver layer but filters for invalid records.

# COMMAND ----------

@dlt.table(
    name="customers_quarantine",
    comment="Customer records that failed DQX quality checks",
    table_properties={
        "quality": "quarantine"
    }
)
@dlt.expect_or_drop("has_dqx_errors", "_errors IS NOT NULL")
def customers_quarantine(engine: DQEngine = dq_engine) -> DataFrame:
    # Read from bronze layer
    bronze_df = dlt.read_stream("customers_bronze")
    
    # Load rules from YAML
    yaml_file = f"{rules_location}/customers_rules.yaml"
    checks = yaml.safe_load(yaml_file)
    
    # Apply DQX checks using the shared DQEngine
    df_with_checks = engine.apply_checks_by_metadata(bronze_df, checks)
    
    # Add quarantine metadata
    return df_with_checks.select(
        "*",
        F.current_timestamp().alias("quarantine_timestamp"),
        F.concat_ws(", ", F.col("_errors")).alias("error_details"),
        F.size(F.col("_errors")).alias("error_count")
    )

# COMMAND ----------

@dlt.table(
    name="products_quarantine",
    comment="Product records that failed DQX quality checks",
    table_properties={
        "quality": "quarantine"
    }
)
@dlt.expect_or_drop("has_dqx_errors", "_errors IS NOT NULL")
def products_quarantine(engine: DQEngine = dq_engine) -> DataFrame:
    # Read from bronze layer
    bronze_df = dlt.read_stream("products_bronze")
    
    # Load rules from YAML
    yaml_file = f"{rules_location}/products_rules.yaml"
    checks = yaml.safe_load(yaml_file)
    
    # Apply DQX checks using the shared DQEngine
    df_with_checks = engine.apply_checks_by_metadata(bronze_df, checks)
    
    # Add quarantine metadata
    return df_with_checks.select(
        "*",
        F.current_timestamp().alias("quarantine_timestamp"),
        F.concat_ws(", ", F.col("_errors")).alias("error_details"),
        F.size(F.col("_errors")).alias("error_count")
    )

# COMMAND ----------

@dlt.table(
    name="orders_quarantine",
    comment="Order records that failed DQX quality checks",
    table_properties={
        "quality": "quarantine"
    }
)
@dlt.expect_or_drop("has_dqx_errors", "_errors IS NOT NULL")
def orders_quarantine(engine: DQEngine = dq_engine) -> DataFrame:
    # Read from bronze layer
    bronze_df = dlt.read_stream("orders_bronze")
    
    # Load rules from YAML
    yaml_file = f"{rules_location}/orders_rules.yaml"
    checks = yaml.safe_load(yaml_file)
    
    # Apply DQX checks using the shared DQEngine
    df_with_checks = engine.apply_checks_by_metadata(bronze_df, checks)
    
    # Add quarantine metadata
    return df_with_checks.select(
        "*",
        F.current_timestamp().alias("quarantine_timestamp"),
        F.concat_ws(", ", F.col("_errors")).alias("error_details"),
        F.size(F.col("_errors")).alias("error_count")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer - Business Analytics
# MAGIC 
# MAGIC Create enriched tables for business intelligence and analytics using validated silver data.

# COMMAND ----------

@dlt.table(
    name="orders_gold",
    comment="Enriched order data joined with customer and product information",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.zOrderCols": "order_date"
    }
)
def orders_gold() -> DataFrame:
    orders = dlt.read_stream("orders_silver")
    customers = dlt.read_stream("customers_silver")
    products = dlt.read_stream("products_silver")
    
    return (
        orders
        .join(customers, orders.customer_id == customers.id, "inner")
        .join(products, orders.product_id == products.id, "inner")
        .select(
            orders.id.alias("order_id"),
            orders.order_date,
            orders.status,
            orders.quantity,
            orders.order_amount,
            customers.customer_name,
            customers.email.alias("customer_email"),
            customers.country.alias("customer_country"),
            products.product_name,
            products.category.alias("product_category"),
            products.unit_price.alias("product_unit_price"),
            orders.ingestion_timestamp
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Metrics
# MAGIC 
# MAGIC Create a summary view of data quality metrics based on DQX results.

# COMMAND ----------

@dlt.table(
    name="dqx_quality_summary",
    comment="Summary of DQX data quality metrics with pass/fail rates"
)
def dqx_quality_summary() -> DataFrame:
    """
    Create a summary of data quality metrics by analyzing DQX results.
    """
    
    # Get counts from bronze and silver tables
    customers_bronze = dlt.read("customers_bronze").count()
    products_bronze = dlt.read("products_bronze").count()
    orders_bronze = dlt.read("orders_bronze").count()
    
    customers_silver = dlt.read("customers_silver").count()
    products_silver = dlt.read("products_silver").count()
    orders_silver = dlt.read("orders_silver").count()
    
    customers_quarantine = dlt.read("customers_quarantine").count()
    products_quarantine = dlt.read("products_quarantine").count()
    orders_quarantine = dlt.read("orders_quarantine").count()
    
    # Create summary
    summary_data = [
        ("customers", customers_bronze, customers_silver, customers_quarantine),
        ("products", products_bronze, products_silver, products_quarantine),
        ("orders", orders_bronze, orders_silver, orders_quarantine)
    ]
    
    schema = StructType([
        StructField("table_name", StringType(), False),
        StructField("bronze_records", LongType(), False),
        StructField("silver_records", LongType(), False),
        StructField("quarantine_records", LongType(), False)
    ])
    
    summary_df = spark.createDataFrame(summary_data, schema)
    
    return summary_df.withColumn(
        "quality_pass_rate",
        F.round((F.col("silver_records") / F.col("bronze_records")) * 100, 2)
    ).withColumn(
        "quality_fail_rate",
        F.round((F.col("quarantine_records") / F.col("bronze_records")) * 100, 2)
    ).withColumn(
        "summary_timestamp",
        F.current_timestamp()
    )
