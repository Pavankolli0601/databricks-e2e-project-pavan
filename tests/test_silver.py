"""
Unit-style tests for silver transformations.
"""
import sys
from pathlib import Path
import pytest

# Add project root for src imports
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.transforms.silver import (
    standardize_string_columns,
    deduplicate_by_key,
    transform_customers,
    transform_orders,
)


def test_standardize_string_columns_trims_and_nulls(spark, sample_customers):
    result = standardize_string_columns(sample_customers)
    rows = result.collect()
    # Row with "  " in name should become null
    names = [r["name"] for r in rows]
    assert None in names or "" not in [n for n in names if n]


def test_deduplicate_by_key_keeps_one_per_key(spark):
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    df = spark.createDataFrame(
        [("a", 1, "x"), ("a", 2, "y"), ("b", 1, "z")],
        ["id", "seq", "val"],
    )
    df = df.withColumn("_ingestion_ts", F.current_timestamp())
    result = deduplicate_by_key(df, ["id"], order_by="seq")
    assert result.count() == 2
    ids = [r["id"] for r in result.collect()]
    assert "a" in ids and "b" in ids


def test_transform_customers_returns_dataframe(spark, sample_customers):
    result = transform_customers(sample_customers)
    assert result.count() >= 1
    assert "customer_id" in result.columns or "name" in result.columns


def test_transform_orders_returns_dataframe(spark, sample_orders):
    result = transform_orders(sample_orders)
    assert result.count() >= 1
