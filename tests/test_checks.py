"""
Unit-style tests for data quality checks.
"""
import sys
from pathlib import Path
import pytest

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.quality.checks import check_row_count, check_nulls, check_duplicates, run_quality_checks


def test_check_row_count_pass(spark, sample_customers):
    r = check_row_count(sample_customers, min_rows=0)
    assert r["passed"] is True
    assert r["value"] == 3


def test_check_row_count_fail(spark, sample_customers):
    r = check_row_count(sample_customers, min_rows=10)
    assert r["passed"] is False


def test_check_nulls(spark):
    df = spark.createDataFrame([("a", None), ("b", "x")], ["id", "val"])
    r = check_nulls(df, columns=["val"], max_null_pct=60.0)
    assert r["passed"] is True
    assert "val" in r.get("value", {})


def test_check_duplicates(spark):
    df = spark.createDataFrame([("a",), ("a",), ("b",)], ["id"])
    r = check_duplicates(df, ["id"])
    assert r["passed"] is False
    assert r["value"] == 1  # one duplicate row


def test_run_quality_checks(spark, sample_customers):
    out = run_quality_checks(
        sample_customers,
        name="customers",
        key_columns=["customer_id"],
        min_rows=0,
    )
    assert out["dataset"] == "customers"
    assert "row_count" in out["checks"]
    assert "nulls" in out["checks"]
    assert "duplicates" in out["checks"]
