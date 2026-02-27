"""
Write DataFrames to Delta Lake.
Paths are resolved via utils.config.resolve_path when using relative base.
"""
from pyspark.sql import DataFrame


def write_delta(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    overwrite_schema: bool = True,
) -> str:
    """
    Write DataFrame to Delta at path.
    mode: overwrite | append
    On Databricks, path can be absolute e.g. /dbfs/mnt/lake/silver/table_name
    """
    from src.utils.config import resolve_path
    full_path = resolve_path(path) if not path.startswith("/") else path
    df.write.format("delta").mode(mode).option(
        "overwriteSchema", str(overwrite_schema).lower()
    ).save(full_path)
    return full_path


def write_bronze(df: DataFrame, base_path: str, table_name: str, mode: str = "overwrite") -> str:
    """Convenience: write to bronze/base_path/table_name."""
    from pathlib import Path
    from src.utils.config import resolve_path
    full = Path(resolve_path(base_path)) / table_name
    return write_delta(df, str(full), mode=mode)


def write_silver(df: DataFrame, base_path: str, table_name: str, mode: str = "overwrite") -> str:
    """Convenience: write to silver/base_path/table_name."""
    from pathlib import Path
    from src.utils.config import resolve_path
    full = Path(resolve_path(base_path)) / table_name
    return write_delta(df, str(full), mode=mode)


def write_gold(df: DataFrame, base_path: str, table_name: str, mode: str = "overwrite") -> str:
    """Convenience: write to gold/base_path/table_name."""
    from pathlib import Path
    from src.utils.config import resolve_path
    full = Path(resolve_path(base_path)) / table_name
    return write_delta(df, str(full), mode=mode)
