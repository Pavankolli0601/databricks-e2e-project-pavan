# Architecture

Bronze → Silver → Gold with configurable paths and modular transforms.

- **Bronze**: Parquet ingestion from `input_path` into Delta; adds `_ingestion_ts`, `_source_file`.
- **Silver**: Clean/deduplicate dimensions; join orders with customers, products, regions → `silver_orders_enriched`.
- **Gold**: Business metrics tables (revenue by region, top products, CLV, order counts, orders by date).
- **Quality**: Basic checks in `src/quality/checks.py` (row count, nulls, duplicates).

See README.md for diagram and run instructions.
