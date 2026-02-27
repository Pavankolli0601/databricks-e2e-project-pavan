"""
Read parquet and Delta tables.
Paths are resolved via utils.config.resolve_path when using relative base.
"""
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Get active SparkSession or create one."""
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def read_parquet(spark: SparkSession, path: str):
    """Read a single path or directory of parquet files."""
    return spark.read.parquet(path)


def read_parquet_glob(spark: SparkSession, base_path: str, pattern: str):
    """
    Read all parquet files matching pattern under base_path.
    base_path can be relative (to project root) or absolute (e.g. Databricks mount).
    pattern e.g. 'customer*.parquet'.
    """
    from pathlib import Path
    from src.utils.config import resolve_path
    full = Path(resolve_path(base_path)) / pattern
    return spark.read.parquet(str(full))


def read_delta(spark: SparkSession, path: str):
    """Read a Delta table from path."""
    return spark.read.format("delta").load(path)
