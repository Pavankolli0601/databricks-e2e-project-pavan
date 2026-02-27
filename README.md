# Databricks End-to-End Data Engineering Project (Version 2)
![CI](https://github.com/Pavankolli0601/databricks-e2e-project-pavan/actions/workflows/tests.yml/badge.svg)
Enterprise-lite PySpark pipeline: Bronze → Silver → Gold with Delta Lake, modular code, and basic data quality checks.

---

## Overview

This project ingests parquet datasets (customers, orders, products, regions) from a **configurable input path**, writes raw data to **Bronze** Delta tables, cleans and joins in **Silver**, and produces **Gold** business metrics. All paths are configurable via YAML so the same code runs locally or on Databricks with a mount/volume.

**Stack:** PySpark, Delta Lake, YAML config, pytest for transformation/quality tests.

---

## Architecture (ASCII)

```
                    ┌──────────────────────────────────────────────────────────────┐
                    │                     CONFIG (configs/paths.yaml)              │
                    │  input_path, bronze_path, silver_path, gold_path             │
                    └──────────────────────────────────────────────────────────────┘
                                                │
  ┌─────────────┐     ┌─────────────┐     ┌─────▼─────┐     ┌─────────────┐
  │   PARQUET   │     │   BRONZE    │     │  SILVER   │     │    GOLD     │
  │ (input_path)│────▶│   Delta     │────▶│  Delta    │────▶│   Delta     │
  │ customers*, │     │ + metadata  │     │ clean +   │     │ metrics     │
  │ orders*,    │     │ bronze_*    │     │ join      │     │ revenue,    │
  │ products*,  │     │             │     │ silver_*   │     │ top products│
  │ regions*    │     │             │     │ orders_   │     │ CLV, counts │
  └─────────────┘     └─────────────┘     │ enriched  │     └─────────────┘
                                          └─────────────┘
                                                │
                                          ┌─────▼─────┐
                                          │  QUALITY  │
                                          │ checks.py │
                                          └───────────┘
```

- **Bronze:** Raw ingestion; adds `_ingestion_ts`, `_source_file`. No business logic.
- **Silver:** Standardize strings, deduplicate by key, join orders with customers/products/regions → `silver_orders_enriched`.
- **Gold:** Aggregated metrics (revenue by region, top products, customer lifetime value proxy, order counts, orders by date).
- **Quality:** Row count, null %, duplicate-key checks on silver (and optionally gold).

---

## Project structure

```
configs/           # Paths and table names (change for Databricks)
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
    readers.py     # read_parquet_glob, read_delta
    writers.py     # write_delta, write_bronze/silver/gold
  transforms/
    silver.py      # clean, dedupe, join_orders_enriched
    gold.py        # revenue_by_region, top_products, CLV, order_counts, orders_by_date
  quality/
    checks.py      # check_row_count, check_nulls, check_duplicates, run_quality_checks
  utils/
    config.py      # load_config, resolve_path
tests/
  conftest.py      # spark, sample_customers, sample_orders
  test_silver.py
  test_gold.py
  test_checks.py
requirements.txt
README.md
```

---

## How to run on Databricks

1. **Upload project** to the workspace (repo clone or folder import). Ensure the project root is the working directory (or add it to the Python path in each notebook).

2. **Configure paths** in `configs/paths.yaml` for your lake/mount, for example:
   ```yaml
   base_path: "/dbfs/mnt/lake"           # or /Volumes/catalog/schema/volume
   input_path: "/dbfs/mnt/lake/raw"
   bronze_path: "/dbfs/mnt/lake/bronze"
   silver_path: "/dbfs/mnt/lake/silver"
   gold_path: "/dbfs/mnt/lake/gold"
   ```
   Place your parquet files under `input_path` with names matching the `sources` globs (e.g. `customer*.parquet`, `orders*.parquet`).

3. **Run notebooks in order** on a cluster with PySpark + Delta:
   - `01_bronze_ingest.py` – ingest parquet to Delta bronze
   - `02_silver_transform.py` – silver clean and join
   - `03_gold_aggregations.py` – gold metrics
   - `04_data_quality_checks.py` – run quality checks

4. **Cluster config:** Use a runtime that includes Delta (e.g. Databricks Runtime). No need to install packages if using the repo’s `requirements.txt` as a cluster library; otherwise install `pyspark`, `delta-spark`, `pyyaml` on the cluster.

---

## Configuration example

```yaml
# configs/paths.yaml – local
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

# On Databricks, override with absolute paths:
# input_path: "/dbfs/mnt/lake/raw"
# bronze_path: "/dbfs/mnt/lake/bronze"
# ...
```

Paths can be relative (resolved from project root) or absolute. Use absolute paths for Databricks mounts/volumes so the same notebooks work without changing code.

---

## Tables produced

| Layer   | Tables |
|--------|--------|
| **Bronze** | `bronze_customers`, `bronze_orders`, `bronze_products`, `bronze_regions` (raw + `_ingestion_ts`, `_source_file`) |
| **Silver** | `silver_customers`, `silver_orders`, `silver_products`, `silver_regions`, `silver_orders_enriched` (orders joined with dimensions) |
| **Gold**  | `gold_revenue_by_region`, `gold_top_products`, `gold_customer_lifetime_value`, `gold_order_counts`, `gold_orders_by_date` |

Schema details depend on your source parquet; column names in code (e.g. `customer_id`, `amount`, `order_date`) are inferred or can be adjusted in `src/transforms/silver.py` and `src/transforms/gold.py`.

---

## What we improved in V2 vs V1

| Area | V1 | V2 |
|------|----|----|
| **Layout** | Monolithic scripts / single .dbc | Clear split: `configs/`, `notebooks/`, `src/`, `tests/`, `docs/` |
| **I/O** | Inline read/write in pipeline | Dedicated `src/io/readers.py` and `writers.py`; configurable paths |
| **Transforms** | Mixed in notebooks | `src/transforms/silver.py` and `gold.py`; reusable and testable |
| **Quality** | None | `src/quality/checks.py` + notebook `04_data_quality_checks.py` |
| **Config** | Hardcoded or minimal | YAML in `configs/`; `src/utils/config.py`; easy Databricks overrides |
| **Gold** | Generic metrics | Explicit business tables: revenue by region, top products, CLV proxy, order counts, orders by date |
| **Tests** | None | `tests/` with unit-style tests for silver, gold, and quality checks |
| **Docs** | Short README | Senior-level README: architecture, run instructions, config example, table list, V2 vs V1 |

---

## Running tests locally

From the project root (with `requirements.txt` installed):

```bash
pytest tests/ -v
```

Use a local Spark session (see `tests/conftest.py`).

---

## Author

Pavan Kolli
