"""
Basic data quality checks: nulls, row counts, duplicates.
Returns dict of check name -> {passed: bool, message: str, value: optional}.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def check_row_count(df: DataFrame, min_rows: int = 0) -> dict:
    """Ensure DataFrame has at least min_rows."""
    n = df.count()
    passed = n >= min_rows
    return {
        "passed": passed,
        "message": f"Row count {n} >= {min_rows}" if passed else f"Row count {n} < {min_rows}",
        "value": n,
    }


def check_nulls(df: DataFrame, columns: list = None, max_null_pct: float = 100.0) -> dict:
    """
    Check null percentage in columns. If columns is None, use all columns.
    Fails if any column has null_pct > max_null_pct.
    """
    if columns is None:
        columns = df.columns
    total = df.count()
    if total == 0:
        return {"passed": True, "message": "No rows", "value": None}
    results = {}
    for c in columns:
        if c not in df.columns:
            continue
        null_count = df.filter(F.col(c).isNull()).count()
        pct = (null_count / total * 100) if total else 0
        results[c] = {"null_count": null_count, "null_pct": pct}
    worst = max(results.values(), key=lambda x: x["null_pct"]) if results else {"null_pct": 0}
    passed = worst["null_pct"] <= max_null_pct
    return {
        "passed": passed,
        "message": f"Null check: worst null_pct={worst['null_pct']:.2f}% (max allowed {max_null_pct}%)",
        "value": results,
    }


def check_duplicates(df: DataFrame, key_columns: list) -> dict:
    """Check for duplicate keys. Fails if duplicate count > 0."""
    dup_count = df.groupBy(key_columns).count().filter(F.col("count") > 1).count()
    # Total duplicate rows
    total = df.count()
    distinct = df.select(key_columns).distinct().count()
    duplicate_rows = total - distinct if total else 0
    passed = duplicate_rows == 0
    return {
        "passed": passed,
        "message": f"Duplicate keys: {duplicate_rows} duplicate rows on {key_columns}",
        "value": duplicate_rows,
    }


def run_quality_checks(
    df: DataFrame,
    name: str,
    key_columns: list = None,
    min_rows: int = 0,
    null_columns: list = None,
    max_null_pct: float = 100.0,
) -> dict:
    """
    Run a small suite of checks on a DataFrame.
    Returns { check_name: { passed, message, value } }.
    """
    results = {}
    results["row_count"] = check_row_count(df, min_rows=min_rows)
    results["nulls"] = check_nulls(df, columns=null_columns, max_null_pct=max_null_pct)
    if key_columns and all(k in df.columns for k in key_columns):
        results["duplicates"] = check_duplicates(df, key_columns)
    return {"dataset": name, "checks": results}
