"""
Pytest fixtures: Spark session and sample DataFrames for unit-style tests.
"""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Shared Spark session for tests."""
    return (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


@pytest.fixture
def sample_customers(spark):
    """Minimal customers DataFrame."""
    return spark.createDataFrame(
        [
            ("c1", "Alice", "US"),
            ("c2", "Bob", "UK"),
            ("c3", "  ", "DE"),
        ],
        ["customer_id", "name", "country"],
    )


@pytest.fixture
def sample_orders(spark):
    """Minimal orders DataFrame with customer_id and amount."""
    return spark.createDataFrame(
        [
            ("o1", "c1", 100.0, "2024-01-01"),
            ("o2", "c1", 50.0, "2024-01-02"),
            ("o3", "c2", 75.0, "2024-01-01"),
        ],
        ["order_id", "customer_id", "amount", "order_date"],
    )
