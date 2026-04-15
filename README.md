# RetailPulse – End-to-End Sales Analytics Pipeline

RetailPulse is a production-style analytics project that ingests retail sales data, applies a reusable Python ETL pipeline, and publishes insights-ready datasets that can be connected directly to Power BI. The project is designed to support repeatable daily refreshes and clean handoff from engineering to reporting.

## Project Overview

This project processes a retail sales CSV with the following core fields:

- `Order Date`
- `Sales`
- `Profit`
- `Region`
- `Category`
- `Sub-Category`
- `Customer Segment`

The pipeline standardizes the dataset, enriches it with derived business metrics, and produces curated output tables for dashboard development.

## Architecture

`Raw CSV -> ETL Pipeline -> Cleaned Dataset -> Analysis Outputs -> Power BI Dashboard`

### Flow

1. `scripts/etl.py` loads and cleans raw retail sales data.
2. Derived fields such as `Month` and `Profit Margin` are created.
3. The transformed dataset is saved as `data/cleaned_data.csv`.
4. `scripts/analysis.py` generates dashboard-ready summary tables.
5. `scripts/refresh.py` orchestrates the full pipeline and writes execution logs.

## Project Structure

```text
RetailPulse-Analytics/
|
|-- data/
|   |-- raw_sales.csv
|   `-- cleaned_data.csv
|-- scripts/
|   |-- etl.py
|   |-- analysis.py
|   `-- refresh.py
|-- dashboard/
|   |-- top_5_products_by_sales.csv
|   |-- region_wise_total_sales.csv
|   `-- monthly_sales_trends.csv
|-- screenshots/
|-- README.md
|-- requirements.txt
`-- refresh.log
```

## Tech Stack

- Python 3.10+
- Pandas
- NumPy
- Matplotlib
- Seaborn
- Power BI for dashboard consumption

## Setup Instructions

### 1. Clone or open the project

Place the project in your working directory or open the existing folder:

```powershell
cd RetailPulse-Analytics
```

### 2. Create and activate a virtual environment

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 3. Install dependencies

```powershell
pip install -r requirements.txt
```

### 4. Run the ETL pipeline

```powershell
python scripts/etl.py
```

### 5. Run the analysis module

```powershell
python scripts/analysis.py
```

### 6. Run the full automated refresh

```powershell
python scripts/refresh.py
```

## Key Outputs

### ETL output

- `data/cleaned_data.csv`

This file includes:

- standardized dates
- duplicate removal
- handled missing values
- `Month` in `YYYY-MM` format
- `Profit Margin` as `Profit / Sales`

### Analysis outputs

- `dashboard/top_5_products_by_sales.csv`
- `dashboard/region_wise_total_sales.csv`
- `dashboard/monthly_sales_trends.csv`

These outputs are optimized for direct loading into Power BI visuals and KPIs.

## Key Insights

- Top-performing products can be identified by total sales contribution.
- Region-level aggregation highlights the strongest geographic markets.
- Monthly sales trends support seasonality analysis and executive reporting.

Note: the source schema does not include a dedicated `Product` column, so the project uses `Sub-Category` as the product-level entity unless a `Product` field is added later. This makes the pipeline reusable for both the provided schema and richer datasets.

## Automation

`scripts/refresh.py` is designed for scheduled execution using Windows Task Scheduler or cron-compatible schedulers. It:

- runs ETL and analysis in sequence
- logs successes and failures
- writes operational details to `refresh.log`

Example Task Scheduler command:

```powershell
python E:\Data Analysis\New folder\RetailPulse-Analytics\scripts\refresh.py
```

## Power BI Integration

Power BI can connect directly to:

- `data/cleaned_data.csv` for model-level analysis
- files in `dashboard/` for lightweight reporting tables

Recommended visuals:

- line chart for monthly sales trend
- bar chart for top 5 products by sales
- map or clustered bar chart for region-wise sales
- KPI cards for total sales, total profit, and average profit margin

## Code Quality Highlights

- modular functions with docstrings
- structured logging
- reusable CLI-friendly scripts
- validation for missing required columns
- error handling suitable for scheduled jobs
- PEP 8-aligned naming and layout

## Future Improvements

- Azure Blob Storage integration for cloud-based ingestion and archival
- parameterized configuration using `.env` or YAML
- unit tests for ETL and analytics modules
- orchestration with Airflow or Azure Data Factory
- incremental refresh logic for large datasets
- data quality checks with Great Expectations

## Optional Azure Extension

The current code is intentionally modular so Azure Blob Storage support can be added by replacing the file input/output layer without rewriting the transformation logic.
