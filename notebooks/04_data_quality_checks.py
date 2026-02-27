# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Checks
# MAGIC Runs basic checks (row count, nulls, duplicates) on silver and optionally gold tables.

# COMMAND ----------

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# COMMAND ----------

from src.utils.config import load_config, resolve_path
from src.io.readers import get_spark, read_delta
from src.quality.checks import run_quality_checks

# COMMAND ----------

config = load_config()
spark = get_spark()
silver_path = config["silver_path"]
st = config["silver_tables"]

# COMMAND ----------

# Run checks on silver_customers (adjust key_columns to match your schema)
path = resolve_path(silver_path, st["customers"])
df = read_delta(spark, path)
result = run_quality_checks(
    df,
    name="silver_customers",
    key_columns=["customer_id"] if "customer_id" in df.columns else None,
    min_rows=0,
    null_columns=None,
    max_null_pct=100.0,
)
print(result)

# COMMAND ----------

# Run checks on silver_orders_enriched
path_enriched = resolve_path(silver_path, st["orders_enriched"])
df_enriched = read_delta(spark, path_enriched)
result_enriched = run_quality_checks(
    df_enriched,
    name="silver_orders_enriched",
    key_columns=["order_id"] if "order_id" in df_enriched.columns else None,
    min_rows=0,
    max_null_pct=100.0,
)
print(result_enriched)
