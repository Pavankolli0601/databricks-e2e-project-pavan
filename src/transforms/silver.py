"""
Silver layer: clean and standardize columns, deduplicate, join orders with dimensions.
Input: DataFrames from bronze (or raw). Output: cleaned and enriched DataFrames.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def standardize_string_columns(df: DataFrame, columns: list = None) -> DataFrame:
    """Trim whitespace and normalize empty strings to null for string columns."""
    if columns is None:
        columns = [c for c, d in df.dtypes if d == "string"]
    for c in columns:
        if c in df.columns:
            df = df.withColumn(
                c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))
            )
    return df


def standardize_numeric_columns(df: DataFrame, columns: list = None) -> DataFrame:
    """Cast numeric-like columns to double; nulls for non-numeric. Optional: pass list of column names."""
    if columns is None:
        return df
    for c in columns:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))
    return df


def deduplicate_by_key(df: DataFrame, key_columns: list, order_by: str = "_ingestion_ts") -> DataFrame:
    """Keep one row per key (latest by order_by). Drops _rn after."""
    if order_by not in df.columns:
        return df.dropDuplicates(key_columns)
    w = Window.partitionBy(key_columns).orderBy(F.col(order_by).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def add_ingestion_metadata(df: DataFrame):
    """Add _ingestion_ts and _source_file for bronze lineage."""
    return df.withColumn("_ingestion_ts", F.current_timestamp()).withColumn(
        "_source_file", F.input_file_name()
    )


def transform_customers(bronze_customers: DataFrame) -> DataFrame:
    """Clean and deduplicate customers. Key: customer_id (or id)."""
    df = standardize_string_columns(bronze_customers)
    key = "customer_id" if "customer_id" in df.columns else "id"
    if key in df.columns:
        df = deduplicate_by_key(df, [key])
    return df


def transform_products(bronze_products: DataFrame) -> DataFrame:
    """Clean and deduplicate products. Key: product_id (or id)."""
    df = standardize_string_columns(bronze_products)
    key = "product_id" if "product_id" in df.columns else "id"
    if key in df.columns:
        df = deduplicate_by_key(df, [key])
    return df


def transform_regions(bronze_regions: DataFrame) -> DataFrame:
    """Clean and deduplicate regions. Key: region_id (or id)."""
    df = standardize_string_columns(bronze_regions)
    key = "region_id" if "region_id" in df.columns else "id"
    if key in df.columns:
        df = deduplicate_by_key(df, [key])
    return df


def transform_orders(bronze_orders: DataFrame) -> DataFrame:
    """Clean and deduplicate orders. Key: order_id (or id)."""
    df = standardize_string_columns(bronze_orders)
    key = "order_id" if "order_id" in df.columns else "id"
    if key in df.columns:
        df = deduplicate_by_key(df, [key])
    return df


def join_orders_enriched(
    silver_orders: DataFrame,
    silver_customers: DataFrame,
    silver_products: DataFrame,
    silver_regions: DataFrame,
    customer_id_col: str = "customer_id",
    product_id_col: str = "product_id",
    region_id_col: str = "region_id",
) -> DataFrame:
    """
    Join orders with customers, products, regions (left join).
    Adjust column names to match your schema (customer_id, product_id, region_id).
    """
    o = silver_orders.alias("o")
    c = silver_customers.alias("c")
    p = silver_products.alias("p")
    r = silver_regions.alias("r")

    joined = o
    if customer_id_col in silver_orders.columns:
        joined = joined.join(
            c, F.col(f"o.{customer_id_col}") == F.col("c.customer_id"), "left"
        )
    if product_id_col in silver_orders.columns:
        joined = joined.join(
            p, F.col(f"o.{product_id_col}") == F.col("p.product_id"), "left"
        )
    if region_id_col in silver_orders.columns:
        joined = joined.join(
            r, F.col(f"o.{region_id_col}") == F.col("r.region_id"), "left"
        )
    return joined
