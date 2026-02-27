# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze – Ingest Parquet to Delta
# MAGIC Reads parquet from configurable input path and writes Delta tables.
# MAGIC **Configure** `configs/paths.yaml`: set `input_path` (e.g. `/dbfs/mnt/lake/raw`) for Databricks.

# COMMAND ----------

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# COMMAND ----------

from src.utils.config import load_config, resolve_path
from src.io.readers import get_spark, read_parquet_glob
from src.io.writers import write_bronze
from src.transforms.silver import add_ingestion_metadata

# COMMAND ----------

config = load_config()
spark = get_spark()
input_path = config["input_path"]
bronze_path = config["bronze_path"]
sources = config["sources"]
bronze_tables = config["bronze_tables"]

# COMMAND ----------

for entity, pattern in sources.items():
    table_name = bronze_tables[entity]
    df = read_parquet_glob(spark, input_path, pattern)
    df = add_ingestion_metadata(df)
    out_path = write_bronze(df, bronze_path, table_name)
    print(f"Bronze written: {entity} -> {out_path}")

# COMMAND ----------

# Optional: display sample
path = resolve_path(bronze_path, bronze_tables["customers"])
display(spark.read.format("delta").load(path).limit(10))
