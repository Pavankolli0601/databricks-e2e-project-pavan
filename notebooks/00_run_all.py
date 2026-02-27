# Databricks notebook source
# MAGIC %md
# MAGIC # Run Full Pipeline (Bronze → Silver → Gold)
# MAGIC Executes all three layers in sequence.

# COMMAND ----------

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# COMMAND ----------

from src.bronze import run_bronze_pipeline, get_spark
from src.silver import run_silver_pipeline
from src.gold import run_gold_pipeline

# COMMAND ----------

spark = get_spark()
run_bronze_pipeline(spark)
run_silver_pipeline(spark)
run_gold_pipeline(spark)
print("Pipeline complete: Bronze -> Silver -> Gold")
