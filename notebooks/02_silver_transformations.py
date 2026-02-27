# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer – Clean & Transform
# MAGIC Reads bronze Delta tables, cleans/deduplicates, and builds enriched order view.

# COMMAND ----------

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# COMMAND ----------

from src.silver import run_silver_pipeline, get_spark

# COMMAND ----------

spark = get_spark()
tables = run_silver_pipeline(spark)
print("Silver tables written:", tables)

# COMMAND ----------

# Optional: display silver orders enriched
from src.config import load_config, resolve_path
config = load_config()
silver_path = config["silver_path"]
st = config["silver_tables"]
path = resolve_path(silver_path, st["orders_enriched"])
df = spark.read.format("delta").load(path)
display(df.limit(20))
