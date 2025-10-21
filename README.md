# Data Quality Demo with DQX on Databricks

## Overview

Data quality fosters trust in data-driven decisions and ensures downstream analytics, ML models, and business processes operate on accurate, complete, and consistent data. Poor data quality can lead to incorrect predictions, compliance issues, and lost business value.

## What is DQX?

[DQX (Data Quality eXtended)](https://databrickslabs.github.io/dqx/) is an open-source framework for data quality in PySpark. DQX enables you to define, monitor, and address data quality issues with support for row/column level rules, in-flight data validation, profiling, and built-in quality dashboards.

## About This Demo

This demo shows you how to build a data quality pipeline using the DQX framework and Databricks Lakeflow Declarative pipelines. We generate a simulated sales order dataset with some intentional data quality issues, then process it through a medallion architecture (bronze → silver → gold → quarantine) while applying DQX checks to filter out invalid records and track data quality metrics.

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
│  │ customers_silver │  │ products_silver  │  │ orders_silver    │      │
│  │ (_valid=true)    │  │ (_valid=true)    │  │ (_valid=true)    │      │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘      │
│           │                     │                     │                │
│  ┌────────┴─────────┐  ┌────────┴─────────┐  ┌────────┴─────────┐      │
│  │ QUARANTINE       │  │ QUARANTINE       │  │ QUARANTINE       │      │
│  │ customers_quar   │  │ products_quar    │  │ orders_quar      │      │
│  │ (_valid=false)   │  │ (_valid=false)   │  │ (_valid=false)   │      │
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

The demo consists of:

1. **Data Generation Notebook** (`notebooks/01_generate_data.py`): Uses Databricks Labs' data generator to create a multi-table sales dataset (customers, products, orders) with randomly injected quality issues such as:
   - Missing values (`NULL` fields)
   - Blank strings
   - Negative currency amounts
   - Invalid values

2. **DQX Pipeline Notebook** (`notebooks/02_dqx_pipeline.py`): A PySpark-based Lakeflow Declarative pipeline that:
   - Ingests CSV files using [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/) into bronze layer streaming tables (no quality checks)
   - **Applies DQX quality checks at the silver layer** using metadata-driven rules from YAML configuration
   - Loads YAML rules using `yaml.safe_load()` from the `rules_location` pipeline parameter
   - Uses a shared `DQEngine` instance passed to all table functions for efficiency
   - DQX `apply_checks()` adds `_errors`, `_warnings`, and `_valid` columns to data
   - **Leverages DLT expectations** (`@dlt.expect_or_drop`) based on the `_valid` column to route records
   - Creates silver layer streaming tables with only valid records (`_valid = true`)
   - Routes invalid records to quarantine tables using DLT expectations (`_valid = false`)
   - Creates enriched gold layer tables for analytics
   - Generates data quality summary metrics

3. **Databricks Asset Bundle**: Infrastructure-as-code configuration that deploys:
   - A serverless Lakeflow pipeline for processing data
   - A daily job that generates new data and runs the pipeline
   - Parameterized configuration for dev/prod environments

4. **Quality Metrics Analysis Notebook** (`notebooks/03_analyze_quality_metrics.py`): A comprehensive data analysis that:
   - Displays current quality summary and pass/fail rates
   - Shows quality trends over time
   - Analyzes quarantined records and identifies issue patterns
   - Provides data quality recommendations based on thresholds

## Data Quality Checks with DQX

The pipeline implements comprehensive DQX checks using the [DQX framework](https://databrickslabs.github.io/dqx/). All checks are defined in **separate YAML configuration files** (one per table) and loaded using **`yaml.safe_load()`** for the metadata-driven approach:

- `config/customers_rules.yaml` - Customer data quality rules
- `config/products_rules.yaml` - Product data quality rules  
- `config/orders_rules.yaml` - Order data quality rules

### DQX + DLT Integration

1. **Bronze Layer**: Raw data ingestion with Auto Loader (no quality checks)
2. **Silver Layer**: DQX `apply_checks()` is applied here, adding quality metadata columns:
   - `_errors`: Array of error messages for failed checks
   - `_warnings`: Array of warning messages  
   - `_valid`: Boolean flag (`true` if no errors, `false` otherwise)
   - DLT expectation `@dlt.expect_or_drop("no_dqx_errors", "_valid = true")` filters valid records
3. **Quarantine Layer**: Same DQX checks applied, but DLT expectation `@dlt.expect_or_drop("has_dqx_errors", "_valid = false")` captures invalid records
4. **Shared DQEngine**: A single `DQEngine` instance is initialized once and passed to all table functions for efficiency

### Customer Data Quality Rules
- **Not Null Checks**: IDs, names, emails, phones, countries, registration dates
- **Email Validation**: Using `row_checks.is_email` for proper format
- **Non-Blank Validation**: Customer names must not be empty strings
- **Criticality**: All rules set to `ERROR` level

### Product Data Quality Rules
- **Not Null Checks**: IDs, names, categories
- **Value Constraints**: Categories must be in ['Electronics', 'Clothing', 'Home', 'Sports', 'Books', 'Toys']
- **Positive Values**: Unit prices must be > 0
- **Non-Negative Values**: Stock quantities must be >= 0
- **Criticality**: All rules set to `ERROR` level

### Order Data Quality Rules
- **Not Null Checks**: IDs, customer IDs, product IDs, order dates
- **Positive Values**: Quantities and amounts must be > 0
- **Value Constraints**: Status must be in ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
- **Non-Blank Validation**: Status strings must not be empty
- **Criticality**: All rules set to `ERROR` level

Records that fail quality checks are automatically routed to **quarantine tables** for analysis and remediation, while valid records flow to silver tables. Quality metrics are tracked in a `dqx_quality_summary` table showing pass/fail rates for each entity.

## Prerequisites

Before deploying this demo, ensure you have:

1. **Databricks Workspace**: A Databricks workspace with Unity Catalog enabled
2. **Databricks CLI**: Install the Databricks CLI (version 0.200.0 or later)
   - Installation guide: https://docs.databricks.com/dev-tools/cli/databricks-cli.html
3. **Authentication**: Configure authentication to your workspace
   - Authentication guide: https://docs.databricks.com/dev-tools/cli/authentication.html
4. **Unity Catalog Permissions**: Permissions to create catalogs, schemas, volumes, and tables
5. **Compute Permissions**: Ability to create serverless compute and job clusters

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
1. Go to **Workflows** → **Jobs**
2. Find `[dev] Daily Data Quality Refresh`
3. Click **Run now**

## Monitoring and Observability

### View Pipeline Execution

1. Navigate to **Workflows** → **Delta Live Tables** in your Databricks workspace
2. Select `[dev] DQX Data Quality Pipeline`
3. View the pipeline graph, execution history, and data lineage

### Monitor Data Quality Metrics

1. In the DLT pipeline UI, click on any silver table
2. View the **Data Quality** tab to see:
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
├── databricks.yml                     # Main bundle configuration
├── quickstart.sh                      # Automated deployment script
├── .gitignore                         # Git ignore rules
├── config/
│   ├── customers_rules.yaml           # Customer DQX rules (loaded with yaml.safe_load)
│   ├── products_rules.yaml            # Product DQX rules (loaded with yaml.safe_load)
│   ├── orders_rules.yaml              # Order DQX rules (loaded with yaml.safe_load)
│   └── dqx_rules.yaml                 # Combined rules (legacy reference)
├── resources/
│   ├── pipelines.yml                  # DLT pipeline configuration
│   └── jobs.yml                       # Job configuration
└── notebooks/
    ├── 01_generate_data.py            # Data generation notebook
    ├── 02_dqx_pipeline.py             # DQX pipeline notebook (PySpark)
    └── 03_analyze_quality_metrics.py  # Quality metrics analysis notebook
```

## Deploying to Production

To deploy to a production environment:

```bash
# Deploy to production target
databricks bundle deploy --target prod
```

**Important**: Before deploying to production:
1. Update the `prod` target in `databricks.yml` with appropriate configurations
2. Configure a service principal for `run_as` permissions
3. Review and adjust the schedule in `resources/jobs.yml`
4. Set up proper alerting and monitoring
5. Consider enabling more strict quality checks or quarantine patterns

For production best practices, see the [Databricks Asset Bundles production documentation](https://docs.databricks.com/dev-tools/bundles/deployment-modes.html).

## Customization

### Adjusting Data Volume

Modify the `num_rows` variable in `databricks.yml` or pass it as a parameter:

```bash
databricks bundle deploy --target dev --var num_rows=50000
```

### Modifying Quality Checks

Edit the appropriate YAML file in the `config/` directory to add, remove, or modify DQX rules. Rules are loaded using `yaml.safe_load()` at runtime:

**Example: Add a new rule to `config/customers_rules.yaml`**:

```yaml
rules:
  - col_name: email
    name: email_valid_format
    check_func: is_email
    criticality: ERROR  # or WARN
    
  - col_name: age
    name: age_in_valid_range
    check_func: "lambda col: (col >= 18) & (col <= 120)"
    criticality: ERROR
```

**Available check functions**:
- `is_not_null`: Check for non-null values
- `is_email`: Validate email format
- Lambda expressions: `"lambda col: col > 0"` for custom logic
- Lambda with lists: `"lambda col: col.isin(['value1', 'value2'])"`

The YAML files are deployed with the Databricks Asset Bundle and loaded from the workspace file system.

### Adding More Tables

1. Add data generation logic in `notebooks/01_generate_data.py`
2. Create a new YAML file `config/<entity_name>_rules.yaml` with DQX quality rules
3. Add the YAML file to `resources/pipelines.yml` in the `libraries` section
4. Create bronze streaming table using Auto Loader in `notebooks/02_dqx_pipeline.py`
5. Create bronze_dqx table that loads rules and applies DQX checks
6. Create silver table with `@dlt.expect_or_drop("no_dqx_errors", "_valid = true")`
7. Create quarantine table with `@dlt.expect_or_drop("has_dqx_errors", "_valid = false")`
8. Optionally add to the gold layer with joins

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
