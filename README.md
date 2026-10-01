# Databricks Retail Lakehouse

End-to-end retail analytics lakehouse built with Databricks, PySpark, Delta Lake, Unity Catalog, Databricks Jobs, and Databricks SQL.

This project demonstrates a Medallion Architecture pipeline that ingests raw retail data, cleans and validates it, quarantines invalid records, creates analytics-ready Gold tables, and exposes business metrics through a Databricks SQL dashboard.

## Architecture

![Retail Lakehouse Architecture](docs/architecture.png)

## Pipeline Architecture

The solution follows the Bronze → Silver → Gold pattern.

### Bronze Layer

Raw customer and order data is loaded into Delta tables with ingestion metadata.

Tables:

- `customers_raw`
- `orders_raw`

The customer pipeline also demonstrates incremental processing using Delta `MERGE` to update existing customers and insert new customers.

### Silver Layer

Bronze data is cleaned, standardized, deduplicated, and validated.

Tables:

- `customers`
- `orders`

Invalid orders are separated into a quarantine table instead of being allowed into downstream analytics.

### Data Quality

The pipeline runs six automated validation checks:

1. Customer IDs are unique
2. Order IDs are unique
3. Customer IDs contain no null values
4. Order amounts are non-negative
5. Order quantities are positive
6. Expected quarantine count is validated

All six checks must pass before the Gold analytics task runs.

## Gold Analytics

The Gold layer contains business-ready analytical models.

### `customer_360`

Provides a consolidated customer-level view including:

- total orders
- total units purchased
- total revenue
- average order value
- most recent order timestamp
- customer attributes

### `segment_performance`

Aggregates performance by customer segment, including:

- unique customers
- total orders
- units sold
- total revenue
- average order value
- average lifetime value

### `daily_sales_performance`

Provides daily sales metrics including:

- total orders
- unique customers
- units sold
- total revenue
- average order value

## Pipeline Orchestration

The Databricks Job executes four dependent tasks:

```text
01_bronze_ingestion
        ↓
02_silver_transformations
        ↓
03_data_quality
        ↓
04_gold_analytics
```

The tasks run sequentially so analytics tables are created only after upstream ingestion, transformation, and validation succeed.

![Databricks Workflow Run](docs/workflow_run.png)

## Analytics Dashboard

A Databricks SQL dashboard provides a business-facing view of the Gold layer.

Dashboard metrics include:

- Total Orders: **14**
- Total Revenue: **$3,088.00**
- Average Order Value: **$220.57**
- Revenue by Segment
- Daily Sales Trend

![Retail Lakehouse Dashboard](docs/dashboard.png)

## Delta Lake Features Demonstrated

The project uses Delta Lake for:

- ACID-compatible tables
- incremental `MERGE` upserts
- schema enforcement
- table history and versioning
- time travel

For example, the Bronze customer table grew from 10 records in the initial version to 12 records after the incremental customer batch.

## Data Quality and Quarantine

The raw orders dataset contains 17 records.

After validation:

```text
Raw Orders          17
Valid Silver Orders 14
Quarantined Orders   3
```

Invalid records include examples such as missing customer IDs, negative quantities, and negative order amounts.

## Technology Stack

- Databricks
- Apache Spark
- PySpark
- Spark SQL
- Delta Lake
- Unity Catalog
- Databricks Jobs
- Databricks SQL
- Python
- SQL

## Repository Structure

```text
databricks-retail-lakehouse/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformations.py
│   ├── 03_data_quality.py
│   └── 04_gold_analytics.py
│
├── sql/
│   ├── 01_segment_performance.sql
│   ├── 02_daily_sales_trend.sql
│   └── 03_kpi_summary.sql
│
├── docs/
│   ├── architecture.png
│   ├── dashboard.png
│   └── workflow_run.png
│
└── README.md
```

## Running the Project

Run the notebooks in this order:

```text
01_bronze_ingestion
02_silver_transformations
03_data_quality
04_gold_analytics
```

Alternatively, run the `Retail Lakehouse Data Pipeline` Databricks Job to execute the complete workflow.

The project uses a small embedded sample dataset so the full pipeline can be reproduced without external data dependencies.

## Project Highlights

This project demonstrates practical lakehouse engineering concepts including:

- Medallion Architecture
- incremental Delta processing
- Delta `MERGE` upserts
- data-quality gates
- quarantine handling
- Customer 360 modeling
- business-ready Gold aggregations
- Databricks Jobs orchestration
- Databricks SQL analytics
- Delta Lake time travel
- Unity Catalog organization
