# Retail Analytics Data Platform – Databricks End-to-End Project (V2)

![CI](https://github.com/Pavankolli0601/databricks-e2e-project-pavan/actions/workflows/tests.yml/badge.svg)

Retail Analytics Data Platform implemented using Medallion Architecture (Bronze → Silver → Gold) with PySpark, Delta Lake, modular Python packages, automated testing, and GitHub Actions CI.

---

## Executive Overview

This repository models a structured Retail Analytics lakehouse pipeline designed to transform raw batch data into curated, analytics-ready Delta tables.

The project emphasizes:

- Clean modular PySpark design
- Clear Bronze/Silver/Gold separation
- Config-driven execution
- Basic but practical data quality checks
- Unit-tested transformation logic
- CI integration for code validation

The implementation intentionally focuses on clarity and structure rather than advanced performance optimizations, making the architectural patterns easy to follow and extend.

---

## Retail Analytics Use Case

The platform ingests batch parquet feeds representing:

- Customers  
- Orders  
- Products  
- Regions  

These raw feeds are transformed into curated Delta tables that support:

- Revenue by region reporting  
- Top product analysis  
- Customer lifetime value (CLV proxy)  
- Order volume tracking  
- Daily sales trends  

All transformations are implemented using modular Python functions rather than embedded notebook logic.

---

## Actual Repository Structure

```text
configs/
  paths.yaml

docs/
  architecture.md

notebooks/
  01_bronze_ingest.py
  02_silver_transform.py
  03_gold_aggregations.py
  04_data_quality_checks.py

src/
  io/
    readers.py
    writers.py
  transforms/
    silver.py
    gold.py
  quality/
    checks.py
  utils/
    config.py

tests/
  conftest.py
  test_silver.py
  test_gold.py
  test_checks.py

.github/workflows/
  tests.yml
```

The notebooks orchestrate the pipeline.  
All business logic lives in `src/` modules.

---

## Medallion Architecture

```
                    CONFIG (configs/paths.yaml)
          input_path | bronze_path | silver_path | gold_path
                                │
  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
  │   PARQUET   │────▶│   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │
  │ customers   │     │ Raw + Meta  │     │ Cleaned +   │     │ Retail      │
  │ orders      │     │ _ingestion  │     │ Enriched    │     │ Metrics     │
  │ products    │     │ _source     │     │ Joined      │     │ Tables      │
  │ regions     │     │             │     │             │     │             │
  └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                                                        │
                                                  ┌─────▼─────┐
                                                  │  QUALITY  │
                                                  │ Checks    │
                                                  └───────────┘
```

- **Bronze**: Raw parquet ingestion with metadata.
- **Silver**: Standardization, deduplication, and fact enrichment.
- **Gold**: Aggregated retail metrics ready for reporting.
- **Quality**: Sanity checks on curated datasets.

---

## Bronze Layer – Parquet → Delta

**Notebook:** `01_bronze_ingest.py`

- Loads YAML configuration via `load_config()`.
- Reads parquet using `read_parquet_glob()`.
- Adds ingestion metadata:
  - `_ingestion_ts`
  - `_source_file`
- Writes Delta tables using `write_bronze()` in **overwrite mode**.

Bronze tables:
- `bronze_customers`
- `bronze_orders`
- `bronze_products`
- `bronze_regions`

Each run overwrites target paths.  
There is no incremental ingestion or MERGE logic in this implementation.

---

## Silver Layer – Cleaning and Enrichment

**Notebook:** `02_silver_transform.py`  
**Module:** `src/transforms/silver.py`

Key transformation patterns:

- `standardize_string_columns()`  
  - Trims whitespace  
  - Converts empty strings to null  

- `deduplicate_by_key()`  
  - Keeps one row per business key  
  - Prefers latest `_ingestion_ts` when available  

Entity transforms:
- `transform_customers()`
- `transform_products()`
- `transform_regions()`
- `transform_orders()`

Enrichment:
- `join_orders_enriched()`  
  - Left joins orders with customers, products, and regions  
  - Produces `silver_orders_enriched`

Silver tables:
- `silver_customers`
- `silver_orders`
- `silver_products`
- `silver_regions`
- `silver_orders_enriched`

All writes use overwrite mode.

---

## Gold Layer – Retail Metrics

**Notebook:** `03_gold_aggregations.py`  
**Module:** `src/transforms/gold.py`

Aggregations include:

- `gold_revenue_by_region()`  
- `gold_top_products()`  
- `gold_customer_lifetime_value()` (CLV proxy via grouped revenue)  
- `gold_order_counts()`  
- `gold_orders_by_date()`  

Gold tables:
- `gold_revenue_by_region`
- `gold_top_products`
- `gold_customer_lifetime_value`
- `gold_order_counts`
- `gold_orders_by_date`

Aggregations rely on `groupBy()` logic and inferred amount/date columns.  
All outputs are written using overwrite mode.

---

## Data Quality Layer

**Notebook:** `04_data_quality_checks.py`  
**Module:** `src/quality/checks.py`

Checks implemented:

- `check_row_count()`  
- `check_nulls()`  
- `check_duplicates()`  
- `run_quality_checks()` orchestration wrapper  

Checks return structured dictionaries indicating pass/fail status and metrics.  
They are informational and do not block downstream steps.

---

## Configuration

All paths and source patterns are defined in:

```
configs/paths.yaml
```

Example local configuration:

```yaml
base_path: "data"
input_path: "data/raw"
bronze_path: "data/bronze"
silver_path: "data/silver"
gold_path: "data/gold"

sources:
  customers: "customer*.parquet"
  orders: "orders*.parquet"
  products: "products*.parquet"
  regions: "regions*.parquet"
```

On Databricks, paths can be replaced with mount or volume paths.  
The code reads whatever values are supplied in YAML.

---

## Running on Databricks

1. Clone or import repository into workspace  
2. Update `configs/paths.yaml`  
3. Attach cluster with Delta support  
4. Run notebooks sequentially:
   - Bronze  
   - Silver  
   - Gold  
   - Quality  

No scheduler or job orchestration is defined in this repository.

---

## Testing and CI

### Pytest

- Unit tests validate Silver transformations.
- Unit tests validate Gold aggregations.
- Unit tests validate data quality checks.
- Spark session is provided via `conftest.py`.

Run locally:

```
pip install -r requirements.txt
pytest -v
```

### GitHub Actions

`.github/workflows/tests.yml`

- Runs on push and pull request to `main`
- Installs dependencies
- Executes pytest

CI validates transformation logic automatically.

---

## Technical Stack

- PySpark
- Delta Lake
- YAML configuration
- pytest
- GitHub Actions CI

---

## What This Project Demonstrates

- Structured Medallion architecture implementation
- Modular PySpark design
- Clean separation of concerns
- Config-driven portability (local ↔ Databricks)
- Basic data quality validation patterns
- Unit testing for Spark transformations
- CI integration for data pipeline code

This project is positioned as a clear, structured Retail Analytics pipeline suitable for a mid-level Data Engineer portfolio.