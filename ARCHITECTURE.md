# Architecture Deep Dive

## Overview

This document provides a detailed technical overview of the DQX Data Quality Demo architecture, explaining how each component works together to implement a production-ready data quality pipeline on Databricks.

## Technology Stack

- **Databricks Runtime**: 14.3.x LTS with Photon
- **Delta Live Tables**: Lakeflow Declarative pipelines for streaming ETL
- **DQX Framework**: [Databricks Labs DQX](https://databrickslabs.github.io/dqx/) for data quality checks
- **Unity Catalog**: For data governance and storage
- **Auto Loader**: For incremental file ingestion
- **Databricks Asset Bundles**: For infrastructure-as-code deployment

## Pipeline Layers

### 1. Bronze Layer (Raw Ingestion)

**Purpose**: Ingest raw CSV files from Unity Catalog volumes with minimal transformation.

**Implementation**:
- Uses Auto Loader (`cloudFiles` format) for incremental ingestion
- Schema inference and evolution enabled
- Checkpoint locations for exactly-once processing
- Metadata columns added (`ingestion_timestamp`, `source_file`)

**Tables**:
- `customers_bronze`
- `products_bronze`
- `orders_bronze`

**Key Features**:
```python
spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(source_path)
```

### 2. Silver Layer (Validated Data)

**Purpose**: Apply DQX data quality rules and create clean, validated datasets.

**Implementation**:
- DQX rules defined programmatically using `DQColRule`
- Each rule specifies column, check function, and criticality level
- Rules applied as filter conditions on streaming DataFrames
- Invalid records automatically routed to quarantine tables

**Tables**:
- `customers_silver` (validated customer records)
- `products_silver` (validated product records)
- `orders_silver` (validated order records)

**DQX Rule Structure**:
```python
DQColRule(
    col_name="email",
    name="email_valid_format",
    check_func=row_checks.is_email,
    criticality=Criticality.ERROR.value
)
```

**Rule Types**:
- **Nullability**: `row_checks.is_not_null`
- **Email validation**: `row_checks.is_email`
- **Custom checks**: Lambda functions for business rules
- **Value constraints**: Whitelist/range validations

### 3. Quarantine Layer (Failed Validations)

**Purpose**: Capture and analyze records that fail data quality checks.

**Implementation**:
- Inverse filtering of DQX rules
- Additional metadata columns for troubleshooting
- Enables root cause analysis and data remediation

**Tables**:
- `customers_quarantine`
- `products_quarantine`
- `orders_quarantine`

**Metadata Added**:
- `quarantine_timestamp`: When the record was quarantined
- `quarantine_reason`: Why the record failed validation
- All original columns preserved for analysis

### 4. Gold Layer (Business Analytics)

**Purpose**: Create enriched, business-ready datasets for analytics and reporting.

**Implementation**:
- Stream-stream joins between silver tables
- Denormalized structure for query performance
- Optimized with Z-ordering on date columns

**Tables**:
- `orders_gold`: Complete order information with customer and product details

**Join Strategy**:
```python
orders.join(customers, orders.customer_id == customers.id, "inner")
      .join(products, orders.product_id == products.id, "inner")
```

### 5. Quality Metrics Layer

**Purpose**: Track and monitor data quality metrics over time.

**Implementation**:
- Aggregates counts from bronze, silver, and quarantine tables
- Calculates pass/fail rates for each entity
- Enables trend analysis and alerting

**Tables**:
- `dqx_quality_summary`: Pass/fail rates and record counts

**Metrics Tracked**:
- Bronze record count (total ingested)
- Silver record count (passed validation)
- Quarantine record count (failed validation)
- Quality pass rate (percentage)
- Quality fail rate (percentage)

## Data Quality Rules

### Customer Rules (8 rules)

1. **ID Validation**: Not null
2. **Name Validation**: Not null, not blank
3. **Email Validation**: Not null, valid format (`contains @`)
4. **Phone Validation**: Not null
5. **Country Validation**: Not null
6. **Date Validation**: Registration date not null

### Product Rules (6 rules)

1. **ID Validation**: Not null
2. **Name Validation**: Not null, not blank
3. **Category Validation**: Must be in ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Toys']
4. **Price Validation**: Must be > 0
5. **Stock Validation**: Must be >= 0

### Order Rules (8 rules)

1. **ID Validation**: Not null
2. **Customer ID Validation**: Not null (referential integrity)
3. **Product ID Validation**: Not null (referential integrity)
4. **Date Validation**: Order date not null
5. **Quantity Validation**: Must be > 0
6. **Amount Validation**: Must be > 0
7. **Status Validation**: Not null, not blank
8. **Status Constraint**: Must be in ['pending', 'processing', 'shipped', 'delivered', 'cancelled']

## Deployment Architecture

### Resource Definitions

#### Delta Live Tables Pipeline
```yaml
serverless: true              # Use serverless compute
continuous: false             # Triggered execution
photon: true                  # Enable Photon acceleration
development: true             # Enhanced logging
channel: CURRENT              # Latest features
```

#### Scheduled Job
```yaml
schedule:
  quartz_cron_expression: "0 0 2 * * ?"  # Daily at 2 AM UTC
  timezone_id: "UTC"
  pause_status: UNPAUSED

tasks:
  - generate_data             # Task 1: Generate CSV files
  - run_dqx_pipeline          # Task 2: Run DLT pipeline
```

### Environment Targets

#### Development (`dev`)
- Schema: `dqx_demo_dev`
- Mode: Development (allows non-immutable changes)
- User isolation for compute

#### Production (`prod`)
- Schema: `dqx_demo_prod`
- Mode: Production (immutable, requires --force for changes)
- Service principal execution
- Restricted workspace path

## Data Generation

### Databricks Labs Data Generator

Uses `dbldatagen` library for realistic synthetic data:

```python
dg.DataGenerator(spark, name="orders", rows=10000, partitions=4)
  .withIdOutput()
  .withColumn("customer_id", "integer", values=customer_ids)
  .withColumn("order_amount", "decimal(10,2)", minValue=10.0, maxValue=5000.0)
```

### Quality Issue Injection

Randomly modifies records to simulate real-world data quality problems:

- **Null values**: 10-15% of records get NULL in required fields
- **Blank strings**: 5-10% get empty string values
- **Negative amounts**: 5-10% get negative prices/quantities
- **Invalid values**: Random invalid category/status values

This realistic simulation enables testing and demonstration of DQX capabilities.

## Monitoring and Observability

### DLT Event Log

Delta Live Tables automatically tracks:
- Pipeline execution history
- Data lineage and dependencies
- Performance metrics
- Error logs and stack traces

### Quality Metrics

The `dqx_quality_summary` table provides:
- Real-time pass/fail rates
- Historical trends
- Anomaly detection capabilities

### Quarantine Analysis

The analysis notebook (`03_analyze_quality_metrics.py`) provides:
- Issue type distribution
- Top failing rules
- Temporal patterns
- Actionable recommendations

## Performance Considerations

### Streaming Optimizations

1. **Auto Loader**: Efficient incremental file discovery and ingestion
2. **Checkpoint Management**: Exactly-once processing guarantees
3. **Schema Evolution**: Handles schema changes automatically

### Delta Optimizations

1. **Z-Ordering**: Improves query performance on date columns
2. **Auto Optimize**: Automatic file compaction and optimization
3. **Photon**: Vectorized execution engine for faster processing

### DQX Performance

1. **Filter-based approach**: Lightweight validation using Spark filters
2. **Streaming-friendly**: Works with both batch and streaming data
3. **Parallel execution**: Rules applied in parallel where possible

## Scalability

### Horizontal Scaling

- **Serverless compute**: Automatically scales based on workload
- **Partitioning**: Data partitioned for parallel processing
- **Streaming**: Processes data incrementally, not all at once

### Vertical Scaling

- **Photon**: Optimized for large data volumes
- **Delta Lake**: Efficient storage format with compression
- **Unity Catalog**: Centralized metadata for fast lookups

## Security and Governance

### Unity Catalog Integration

- **Catalog-level isolation**: Separate catalogs for dev/prod
- **Schema-level organization**: Logical grouping of related tables
- **Volume-based storage**: Managed storage for CSV files
- **RBAC**: Role-based access control on all objects

### Data Lineage

- **Table lineage**: Tracked automatically by DLT
- **Column lineage**: Visible in Unity Catalog
- **Source tracking**: Each record tagged with source file

### Audit Trail

- **Quarantine records**: Complete history of failed validations
- **Quality metrics**: Historical quality trends
- **Event logs**: Pipeline execution audit trail

## Extension Points

### Adding New Rules

1. Define new `DQColRule` in `02_dqx_pipeline.py`
2. Add to appropriate rule list (customer/product/order)
3. Rules automatically applied on next pipeline run

### Adding New Tables

1. Generate data in `01_generate_data.py`
2. Create bronze table with Auto Loader
3. Define DQX rules for validation
4. Create silver and quarantine tables
5. Optionally join into gold layer

### Custom Reactions

DQX supports multiple reaction strategies:
- **Drop**: Remove invalid records (current implementation)
- **Mark**: Tag records but allow through
- **Quarantine**: Route to separate table (current implementation)
- **Fail**: Stop pipeline on validation failure

## Best Practices Demonstrated

1. **Medallion Architecture**: Bronze → Silver → Gold pattern
2. **Quarantine Pattern**: Separate invalid records for analysis
3. **Infrastructure as Code**: Databricks Asset Bundles
4. **Monitoring**: Built-in quality metrics and dashboards
5. **Parameterization**: Configurable catalog/schema/volume
6. **Documentation**: Inline comments and README
7. **Version Control**: Git-friendly structure
8. **Environment Isolation**: Dev/prod separation

## References

- [DQX Documentation](https://databrickslabs.github.io/dqx/)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)
