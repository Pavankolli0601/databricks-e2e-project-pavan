"""
Gold layer: business metrics tables.
Input: silver DataFrames (especially silver_orders_enriched). Output: aggregated DataFrames.
Column names (e.g. amount, quantity, region_id) are inferred where possible; override in calls if needed.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _amount_col(df: DataFrame):
    """Infer revenue/amount column for aggregations."""
    for c in ["amount", "total_amount", "revenue", "order_amount", "quantity"]:
        if c in df.columns:
            return c
    return None


def _date_col(df: DataFrame):
    """Infer date column for time-based aggregations."""
    for c in ["order_date", "date", "created_at", "order_ts"]:
        if c in df.columns:
            return c
    return None


def gold_revenue_by_region(orders_enriched: DataFrame) -> DataFrame:
    """Total revenue by region. Expects region_id (or region name) and an amount column."""
    amt = _amount_col(orders_enriched)
    region_col = "region_id" if "region_id" in orders_enriched.columns else "region_name"
    if region_col not in orders_enriched.columns:
        return orders_enriched.sparkSession.createDataFrame(
            [], "region string, revenue double"
        )
    agg = orders_enriched.groupBy(F.col(region_col).alias("region"))
    if amt:
        return agg.agg(F.sum(amt).alias("revenue"))
    return agg.agg(F.count("*").alias("order_count"))


def gold_top_products(orders_enriched: DataFrame, top_n: int = 20) -> DataFrame:
    """Top N products by revenue (or order count). Expects product_id and amount."""
    amt = _amount_col(orders_enriched)
    if "product_id" not in orders_enriched.columns:
        return orders_enriched.sparkSession.createDataFrame(
            [], "product_id string, revenue double, rank int"
        )
    if amt:
        agg = orders_enriched.groupBy("product_id").agg(F.sum(amt).alias("revenue"))
    else:
        agg = orders_enriched.groupBy("product_id").agg(F.count("*").alias("revenue"))
    return agg.orderBy(F.col("revenue").desc()).limit(top_n)


def gold_customer_lifetime_value(orders_enriched: DataFrame) -> DataFrame:
    """
    Customer-level CLV proxy: total revenue and order count per customer.
    Expects customer_id and amount column.
    """
    amt = _amount_col(orders_enriched)
    if "customer_id" not in orders_enriched.columns:
        return orders_enriched.sparkSession.createDataFrame(
            [], "customer_id string, order_count long, total_revenue double"
        )
    if amt:
        return orders_enriched.groupBy("customer_id").agg(
            F.count("*").alias("order_count"),
            F.sum(amt).alias("total_revenue"),
        )
    return orders_enriched.groupBy("customer_id").agg(
        F.count("*").alias("order_count"),
        F.count("*").alias("total_revenue"),
    )


def gold_order_counts(orders_enriched: DataFrame) -> DataFrame:
    """Single-row summary: total order count and distinct customers."""
    if "customer_id" in orders_enriched.columns:
        return orders_enriched.agg(
            F.count("*").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_customers"),
        )
    return orders_enriched.agg(F.count("*").alias("total_orders"))


def gold_orders_by_date(orders_enriched: DataFrame) -> DataFrame:
    """Order count and optional revenue by date. Uses inferred date column."""
    date_c = _date_col(orders_enriched)
    amt = _amount_col(orders_enriched)
    if not date_c:
        return orders_enriched.agg(F.count("*").alias("total_orders"))
    df = orders_enriched.withColumn("order_date", F.to_date(F.col(date_c)))
    if amt:
        agg = df.groupBy("order_date").agg(
            F.count("*").alias("order_count"),
            F.sum(amt).alias("revenue"),
        )
    else:
        agg = df.groupBy("order_date").agg(F.count("*").alias("order_count"))
    return agg.orderBy("order_date")
