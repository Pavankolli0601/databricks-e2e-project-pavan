# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer – Raw Ingestion
# MAGIC Ingests parquet files from `data/raw` into Delta Lake bronze tables.

# COMMAND ----------

import sys
from pathlib import Path

# Add project root so we can import src
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# COMMAND ----------

from src.bronze import run_bronze_pipeline, get_spark

# COMMAND ----------

spark = get_spark()
results = run_bronze_pipeline(spark)
for entity, path in results:
    print(f"Bronze ingested: {entity} -> {path}")

# COMMAND ----------

# Optional: display sample from one bronze table
from src.config import load_config, resolve_path
config = load_config()
bronze_path = config["bronze_path"]
table_name = config["bronze_tables"]["customers"]
path = resolve_path(bronze_path, table_name)
df = spark.read.format("delta").load(path)
display(df.limit(10))
