# Databricks notebook source
# MAGIC %md
# MAGIC # Silver – Clean, Standardize, Join
# MAGIC Reads bronze Delta tables, cleans/deduplicates, and builds orders_enriched.

# COMMAND ----------

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# COMMAND ----------

from src.utils.config import load_config, resolve_path
from src.io.readers import get_spark, read_delta
from src.io.writers import write_silver
from src.transforms.silver import (
    transform_customers,
    transform_products,
    transform_regions,
    transform_orders,
    join_orders_enriched,
)

# COMMAND ----------

config = load_config()
spark = get_spark()
bronze_path = config["bronze_path"]
silver_path = config["silver_path"]
bt = config["bronze_tables"]
st = config["silver_tables"]

# COMMAND ----------

# Read bronze
bronze_customers = read_delta(spark, resolve_path(bronze_path, bt["customers"]))
bronze_orders = read_delta(spark, resolve_path(bronze_path, bt["orders"]))
bronze_products = read_delta(spark, resolve_path(bronze_path, bt["products"]))
bronze_regions = read_delta(spark, resolve_path(bronze_path, bt["regions"]))

# COMMAND ----------

# Transform
silver_customers = transform_customers(bronze_customers)
silver_products = transform_products(bronze_products)
silver_regions = transform_regions(bronze_regions)
silver_orders = transform_orders(bronze_orders)
# Write dimension tables
write_silver(silver_customers, silver_path, st["customers"])
write_silver(silver_products, silver_path, st["products"])
write_silver(silver_regions, silver_path, st["regions"])
write_silver(silver_orders, silver_path, st["orders"])
# Build and write enriched orders
enriched = join_orders_enriched(silver_orders, silver_customers, silver_products, silver_regions)
write_silver(enriched, silver_path, st["orders_enriched"])
print("Silver tables written:", list(st.values()))

# COMMAND ----------

path = resolve_path(silver_path, st["orders_enriched"])
display(spark.read.format("delta").load(path).limit(20))
