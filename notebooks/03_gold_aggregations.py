# Databricks notebook source
# MAGIC %md
# MAGIC # Gold – Business Metrics
# MAGIC Builds revenue by region, top products, customer lifetime value, order counts, orders by date.

# COMMAND ----------

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# COMMAND ----------

from src.utils.config import load_config, resolve_path
from src.io.readers import get_spark, read_delta
from src.io.writers import write_gold
from src.transforms.gold import (
    gold_revenue_by_region,
    gold_top_products,
    gold_customer_lifetime_value,
    gold_order_counts,
    gold_orders_by_date,
)

# COMMAND ----------

config = load_config()
spark = get_spark()
silver_path = config["silver_path"]
gold_path = config["gold_path"]
st = config["silver_tables"]
gt = config["gold_tables"]

# COMMAND ----------

orders_enriched = read_delta(spark, resolve_path(silver_path, st["orders_enriched"]))

# COMMAND ----------

write_gold(gold_revenue_by_region(orders_enriched), gold_path, gt["revenue_by_region"])
write_gold(gold_top_products(orders_enriched, top_n=20), gold_path, gt["top_products"])
write_gold(gold_customer_lifetime_value(orders_enriched), gold_path, gt["customer_lifetime_value"])
write_gold(gold_order_counts(orders_enriched), gold_path, gt["order_counts"])
write_gold(gold_orders_by_date(orders_enriched), gold_path, gt["orders_by_date"])
print("Gold tables written:", list(gt.values()))

# COMMAND ----------

# Sample: customer lifetime value
path = resolve_path(gold_path, gt["customer_lifetime_value"])
display(spark.read.format("delta").load(path))
