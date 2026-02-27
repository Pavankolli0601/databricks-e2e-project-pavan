"""
Unit-style tests for gold aggregations.
"""
import sys
from pathlib import Path
import pytest

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.transforms.gold import (
    gold_revenue_by_region,
    gold_top_products,
    gold_customer_lifetime_value,
    gold_order_counts,
    gold_orders_by_date,
)


def test_gold_customer_lifetime_value(spark, sample_orders):
    result = gold_customer_lifetime_value(sample_orders)
    assert result.count() == 2  # c1, c2
    row = result.filter("customer_id = 'c1'").first()
    assert row is not None
    assert row["order_count"] == 2
    assert row["total_revenue"] == 150.0


def test_gold_order_counts(sample_orders):
    result = gold_order_counts(sample_orders)
    row = result.first()
    assert row["total_orders"] == 3
    assert row["unique_customers"] == 2


def test_gold_top_products(spark, sample_orders):
    # Add product_id for top_products
    from pyspark.sql import functions as F
    orders = sample_orders.withColumn("product_id", F.lit("p1"))
    result = gold_top_products(orders, top_n=5)
    assert result.count() <= 5


def test_gold_orders_by_date(sample_orders):
    result = gold_orders_by_date(sample_orders)
    assert result.count() >= 1
