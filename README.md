# Data Quality Demo with DQX on Databricks

## Overview

Data quality fosters trust in data-driven decisions and ensures downstream analytics, ML models, and business processes operate on accurate, complete, and consistent data. Poor data quality can lead to incorrect predictions, compliance issues, and lost business value.

## What is DQX?

[DQX (Data Quality eXtended)](https://databrickslabs.github.io/dqx/) is an open-source framework for data quality in PySpark. DQX enables you to define, monitor, and address data quality issues with support for row/column level rules, in-flight data validation, profiling, and built-in quality dashboards.

## About This Demo

This demo shows you how to build a data quality pipeline using the DQX framework and Databricks Lakeflow Declarative Pipelines (LDP). We generate a simulated sales order dataset with some intentional data quality issues, then process it through a medallion architecture (bronze → silver → gold → quarantine) while applying DQX checks to filter out invalid records and track data quality metrics.

## Architecture

> See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed design documentation.

### Data Flow

```
┌────────────────────────────────────────────────────────────────────────┐
│                          DATA GENERATION                               │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ 01_generate_data.py (Databricks Labs Data Generator)           │    │
│  │  • Generates customers, products, orders                       │    │
│  │  • Injects quality issues (10-15% of records)                  │    │
│  └──────────────────────┬─────────────────────────────────────────┘    │
│                         │                                              │
│                         ▼                                              │
│              Unity Catalog Volume (CSV files)                          │
└────────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    DQX DATA QUALITY PIPELINE                           │
│                    (02_dqx_pipeline.py - DLT)                          │
│                                                                        │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐      │
│  │ BRONZE LAYER     │  │ BRONZE LAYER     │  │ BRONZE LAYER     │      │
│  │ customers_bronze │  │ products_bronze  │  │ orders_bronze    │      │
│  │ (Auto Loader)    │  │ (Auto Loader)    │  │ (Auto Loader)    │      │
│  │ No DQX checks    │  │ No DQX checks    │  │ No DQX checks    │      │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘      │
│           │                     │                     │                │
│           ├─────────────────────┼─────────────────────┤                │
│           │                     │                     │                │
│           ▼                     ▼                     ▼                │
│       DQX Rules             DQX Rules             DQX Rules            │
│       (YAML load)           (YAML load)           (YAML load)          │
│       Shared DQEngine       Shared DQEngine       Shared DQEngine      │
│           │                     │                     │                │
│           ├─────────────────────┼─────────────────────┤                │
│           ▼                     ▼                     ▼                │
│  ┌────────┴─────────┐  ┌────────┴─────────┐  ┌────────┴─────────┐      │
│  │ SILVER LAYER     │  │ SILVER LAYER     │  │ SILVER LAYER     │      │
│  │ customers_silver │  │ products_silver  │  │ orders_silver    |◀─┐   │
│  │ (Valid rows)     │  │ (Valid rows)     │  │ (Valid rows)     │  │   │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘  │   │
│           │                     │                     │            │   │
│           │                     │                     │     Automated  │
│           │                     │                     │    Reingestion │
│           │                     │                     │            │   │
│  ┌────────┴─────────┐  ┌────────┴─────────┐  ┌────────┴─────────┐  │   │
│  │ QUARANTINE       │  │ QUARANTINE       │  │ QUARANTINE       │  │   │
│  │ customers_quar   │  │ products_quar    │  │ orders_quar      ├──┘   │
│  │ (Invalid rows)   │  │ (Invalid rows)   │  │ (Invalid rows)   │      │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘      │
│           │                     │                     │                │
│           └─────────────────────┼─────────────────────┘                │
│                                 ▼                                      │
│                    ┌─────────────────────────┐                         │
│                    │    GOLD LAYER           │                         │
│                    │    orders_gold          │                         │
│                    │  (Enriched Analytics)   │                         │
│                    └─────────────────────────┘                         │
│                                                                        │
│                    ┌─────────────────────────┐                         │
│                    │  QUALITY METRICS        │                         │
│                    │  dqx_quality_summary    │                         │
│                    │  (Pass/Fail Rates)      │                         │
│                    └─────────────────────────┘                         │
└────────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
┌────────────────────────────────────────────────────────────────────────┐
│                     MONITORING & ANALYSIS                              │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │ 03_analyze_quality_metrics.py                                  │    │
│  │  • Quality trends and dashboards                               │    │
│  │  • Quarantine analysis                                         │    │
│  │  • Issue type distributions                                    │    │
│  │  • Recommendations                                             │    │
│  └────────────────────────────────────────────────────────────────┘    │
└────────────────────────────────────────────────────────────────────────┘
```

### Components

The demo is bundled as a Databricks Asset Bundle which allows you to deploy and run jobs using the Databricks Command Line Interface (CLI).

#### Bundle Resources

Running the demo deploys the following resources in your Databricks workspace:

1. *Data Processing Pipeline*: A serverless Lakeflow pipeline for processing data from files into business-ready analytics datasets. The pipeline can be run ad-hoc, programatically via the CLI, or triggered from a job.

2. *End-to-end Job*: An hourly serverless job that generates new data and runs the pipeline. The job can be run ad-hoc or programatically via the CLI.

#### Bundle Files

The demo contains the following source code files:

1. *Data Generation Notebook* (`notebooks/01_generate_data.py`): Uses Databricks Labs' data generator to create a multi-table sales dataset (customers, products, orders) with randomly injected quality issues such as:
   - Missing values (`NULL` fields)
   - Blank strings
   - Negative currency amounts
   - Invalid values

2. *DQX Pipeline Notebook* (`notebooks/02_dqx_pipeline.py`): A PySpark-based Lakeflow Declarative pipeline that:
   - Ingests CSV files using [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/) into bronze layer streaming tables (no quality checks)
   - Applies DQX quality checks at the silver layer using metadata-driven rules from YAML configuration; Uses DQX + LDP Expectations to actively quarantine low-quality data and reingest data with specific failures
   - Creates enriched gold layer tables for analytics

3. *Quality Metrics Analysis Notebook* (`notebooks/03_analyze_quality_metrics.py`): A comprehensive data analysis that:
   - Displays current quality summary and pass/fail rates
   - Shows quality trends over time
   - Analyzes quarantined records and identifies issue patterns
   - Provides data quality recommendations based on thresholds

4. *Defining Quality Rules Notebook* (`notebooks/04_create_new_quality_rules.py`): Contains examples for data quality rule definition:
   - Inferring rules from existing data using the `DQProfiler`
   - Storing rules in a Delta table
   - Manually curating inferred rule definitions
   - Defining rules in natural language

## Data Quality Checks with DQX

The pipeline implements comprehensive DQX checks using the [DQX framework](https://databrickslabs.github.io/dqx/). All checks are defined in YAML configuration files (one per table) and loaded dynamically at runtime:

- `config/customers_rules.yaml` - Customer data quality rules
- `config/products_rules.yaml` - Product data quality rules  
- `config/orders_rules.yaml` - Order data quality rules

### DQX + DLT Integration

The pipeline follows a medallion architecture where data is moved through several layers of curation. DQX and LDP expectations provide a complete data quality solution for checking conditions, generating metadata, and quarantining results.

1. *Bronze Layer*: Raw data ingestion with Auto Loader (no quality checks)
2. *Silver Layer*: DQX `apply_checks_by_metadata()` is applied here, adding columns with rich data quality metadata.
   - `_errors`: Array of error messages for failed checks
   - `_warnings`: Array of warning messages

LDP Expectations are be applied to ensure that rows with any `_errors` or `_warnings` are written to a quarantine table.
3. *Reingestion*: Quarantined rows with row-level DQX metadata is automatically reingested using the `_errors` column and `append_flow`
4. *Gold Layer*: Data validated with DQX + Expectations is used to write gold layer aggregations and provide rich, business-ready data.

### Defining DQX Checks in YAML

DQX checks are defined using a YAML format that specifies validation rules for each column. Each rule is a YAML object with the following structure:

```yaml
- criticality: error  # can be 'error' or 'warning'
  name: rule_name
  check:
    function: check_function_name
    arguments: 
      column: column_name
      # additional function-specific arguments
```

### Loading DQX Checks in Pipeline Code

The `02_dqx_pipeline.py` notebook demonstrates how to load and apply YAML-based DQX checks:

#### 1. Initialize DQX Engine (Once)

```python
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

# Initialize DQX engine once at the module level for efficiency
workspace_client = WorkspaceClient()
dq_engine = DQEngine(workspace_client)
```

#### 2. Load Checks from YAML

```python
from databricks.labs.dqx.config import WorkspaceFileChecksStorageConfig

# Get rules location from pipeline configuration
rules_location = spark.conf.get("rules_location")

# Load checks from YAML file
yaml_file = f"{rules_location}/customers_rules.yaml"
checks = dq_engine.load_checks(WorkspaceFileChecksStorageConfig(yaml_file))
```

#### 3. Apply Checks to DataFrame

```python
# Apply DQX checks to add _errors and _warnings columns
df_with_checks = dq_engine.apply_checks_by_metadata(bronze_df, checks)
```

#### 4. Use DLT Expectations for Routing

```python
@dlt.table(name="customers_silver")
@dlt.expect("no_dqx_warnings", "_warnings IS NULL")
@dlt.expect_or_drop("no_dqx_errors", "_errors IS NULL")
def customers_silver(engine: DQEngine = dq_engine):
    bronze_df = dlt.read("customers_bronze")
    yaml_file = f"{rules_location}/customers_rules.yaml"
    checks = engine.load_checks(WorkspaceFileChecksStorageConfig(yaml_file))
    return engine.apply_checks_by_metadata(bronze_df, checks)
```

### Data Quality Summary

In this example all rules are defined with the `'error'` criticality.

#### Customer Data Quality Rules
- *Completeness Checks*: Customer IDs, names, emails, phones, countries, registration dates must be non-null; Customer names must be non-null, non-empty strings
- *Validity Checks*: Using regex pattern `.*@.*\..*` for proper format

#### Product Data Quality Rules
- *Completeness Checks*: Product IDs and names must be non-null
- *Consistency Checks*: Product categories must be in a list of allowed values
- *Validity Checks*: Unit prices and stock quantities must be >= 0 (exclusive)

#### Order Data Quality Rules
- *Completeness Checks*: Order IDs, customer IDs, product IDs, order dates must be non-null; Order statuses must be non-null, non-empty strings
- *Consistency Checks*: Order statuses must be in a list of allowed values
- *Validity Checks*: Order quantities and amounts must be >= 0 (exclusive)

Records that fail quality checks are automatically routed to quarantine tables for remediation. Valid records flow to silver tables and into downstream analytics results. Data quality metrics are tracked in a `dqx_quality_summary` table showing pass/fail rates for each business object.

## Prerequisites

Before deploying this demo, ensure you have:

1. *Databricks Workspace*: A Databricks workspace with Unity Catalog enabled
2. *Databricks CLI*: Install the Databricks CLI (version 0.258 or later)
   - Installation guide: https://docs.databricks.com/dev-tools/cli/databricks-cli.html
3. *Authentication*: Configure authentication to your workspace
   - Authentication guide: https://docs.databricks.com/dev-tools/cli/authentication.html
4. *Unity Catalog Permissions*: Permissions to create catalogs, schemas, volumes, and tables
5. *Compute Permissions*: Ability to create serverless compute and job clusters
6. *Library Installation Permissions*: Ability to install Python libraries from [PyPi](https://pypi.org)

## Installation

### Quick Start Option

For the fastest deployment experience, use the provided quick start script:

```bash
./quickstart.sh
```

This interactive script will guide you through authentication, validation, and deployment.

### Manual Installation

Follow these steps for a manual deployment:

### 1. Install Databricks CLI

```bash
# Using pip
pip install databricks-cli

# Or using Homebrew (macOS)
brew tap databricks/tap
brew install databricks
```

For other installation methods, see the [official documentation](https://docs.databricks.com/dev-tools/cli/install.html).

### 2. Authenticate with Your Workspace

```bash
# Configure authentication
databricks auth login --host https://your-workspace-url.cloud.databricks.com
```

This will open a browser window for OAuth authentication. Alternatively, you can use environment variables or a configuration profile. See [authentication documentation](https://docs.databricks.com/dev-tools/auth/index.html) for more options.

### 3. Clone This Repository

```bash
git clone <repository-url>
cd data-quality-demo
```

### 4. Configure Your Deployment

Edit `databricks.yml` to customize variables if needed:

```yaml
variables:
  catalog: main              # Unity Catalog catalog name
  schema: dqx_demo          # Schema name
  volume: raw_data          # Volume name for CSV files
  num_rows: 10000           # Number of rows to generate
```

### 5. Validate the Bundle

```bash
# Validate the bundle configuration
databricks bundle validate
```

This command checks your bundle configuration for errors and validates that all resources are properly defined.

### 6. Deploy to Development Environment

```bash
# Deploy to dev (default target)
databricks bundle deploy --target dev
```

This command will:
- Upload notebooks to your workspace
- Create the Delta Live Tables pipeline
- Create the scheduled job
- Set up all necessary configurations

For more information on bundle deployment, see the [Databricks Asset Bundles documentation](https://docs.databricks.com/dev-tools/bundles/index.html).

### 7. Run the Data Generation

You can manually trigger the data generation to populate initial data:

```bash
# Run the job to generate data and execute the pipeline
databricks bundle run daily_data_quality_refresh --target dev
```

Alternatively, navigate to the Databricks workspace UI:
1. Go to *Workflows* → *Jobs*
2. Find `[dev] Daily Data Quality Refresh`
3. Click *Run now*

## Monitoring and Observability

### View Pipeline Execution

1. Navigate to *Workflows* → *Delta Live Tables* in your Databricks workspace
2. Select `[dev] DQX Data Quality Pipeline`
3. View the pipeline graph, execution history, and data lineage

### Monitor Data Quality Metrics

1. In the DLT pipeline UI, click on any silver table
2. View the *Data Quality* tab to see:
   - Number of records processed
   - Number of records that passed/failed each constraint
   - Historical trends of data quality metrics

### Analyze Quality Metrics with Notebook

Use the provided analysis notebook to deep dive into quality metrics:

1. Open `notebooks/03_analyze_quality_metrics.py`
2. Run the notebook to view:
   - Current quality summary across all tables
   - Quality trends over time
   - Detailed quarantine record analysis
   - Issue type distributions
   - Data quality recommendations

### Query Quality Tables Directly

You can also query the quality tables directly:

```sql
-- View quality summary
SELECT * FROM main.dqx_demo.dqx_quality_summary ORDER BY summary_timestamp DESC;

-- View quarantined customer records
SELECT * FROM main.dqx_demo.customers_quarantine ORDER BY quarantine_timestamp DESC;

-- View quarantined product records
SELECT * FROM main.dqx_demo.products_quarantine ORDER BY quarantine_timestamp DESC;

-- View quarantined order records
SELECT * FROM main.dqx_demo.orders_quarantine ORDER BY quarantine_timestamp DESC;
```

## Project Structure

```
data-quality-demo/
├── README.md                          # This file
├── ARCHITECTURE.md                    # Detailed architecture documentation
├── databricks.yml                     # Main asset bundle configuration
├── quickstart.sh                      # Automated deployment script
├── config/
│   ├── customers_rules.yaml           # Data quality rules for customers dataset
│   ├── products_rules.yaml            # Data quality rules for products dataset
│   ├── orders_rules.yaml              # Data quality rules for orders dataset
├── resources/
│   ├── pipelines.yml                  # Lakeflow pipeline asset bundle configuration
│   └── jobs.yml                       # Lakeflow job asset bundle configuration
└── notebooks/
    ├── 01_generate_data.py            # Data generation notebook
    ├── 02_dqx_pipeline.py             # DQX pipeline notebook (PySpark)
    ├── 03_analyze_quality_metrics.py  # Data quality analysis notebook
    └── 04_create_new_quality_rules.py # Data quality analysis notebook
```

## Troubleshooting

### Pipeline Fails to Start

- Verify Unity Catalog permissions for the target schema
- Ensure the volume exists and is accessible
- Check that serverless is enabled in your workspace

### Data Generation Errors

- Verify `dbldatagen` package installation
- Check volume write permissions
- Ensure sufficient cluster resources

### Quality Check Failures

- Review the DLT event log for specific constraint violations
- Adjust constraints in the SQL notebook as needed
- Consider changing `ON VIOLATION` actions based on requirements

## Additional Resources

- [DQX Framework Documentation](https://databrickslabs.github.io/dqx/)
- [Declarative Pipelines Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [Expectations Documentation](https://docs.databricks.com/delta-live-tables/expectations.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Databricks Labs GitHub](https://github.com/databrickslabs)

## License

This demo is provided as-is for educational and demonstration purposes.
