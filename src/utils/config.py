"""
Load YAML config from configs/ and resolve paths.
On Databricks: override base_path (or input_path) in the YAML to your mount/volume path.
"""
import os
from pathlib import Path

# Project root (parent of src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIGS_DIR = PROJECT_ROOT / "configs"


def load_config(config_name: str = "paths.yaml") -> dict:
    """Load config from configs/<config_name>. Returns dict; falls back to defaults if missing."""
    config_path = CONFIGS_DIR / config_name
    if not config_path.exists():
        return _default_config()
    try:
        import yaml
        with open(config_path) as f:
            return yaml.safe_load(f) or _default_config()
    except Exception:
        return _default_config()


def _default_config() -> dict:
    """Defaults when config file is missing or unreadable."""
    return {
        "base_path": "data",
        "input_path": "data/raw",
        "bronze_path": "data/bronze",
        "silver_path": "data/silver",
        "gold_path": "data/gold",
        "sources": {
            "customers": "customer*.parquet",
            "orders": "orders*.parquet",
            "products": "products*.parquet",
            "regions": "regions*.parquet",
        },
        "bronze_tables": {
            "customers": "bronze_customers",
            "orders": "bronze_orders",
            "products": "bronze_products",
            "regions": "bronze_regions",
        },
        "silver_tables": {
            "customers": "silver_customers",
            "orders": "silver_orders",
            "products": "silver_products",
            "regions": "silver_regions",
            "orders_enriched": "silver_orders_enriched",
        },
        "gold_tables": {
            "revenue_by_region": "gold_revenue_by_region",
            "top_products": "gold_top_products",
            "customer_lifetime_value": "gold_customer_lifetime_value",
            "order_counts": "gold_order_counts",
            "orders_by_date": "gold_orders_by_date",
        },
    }


def resolve_path(base: str, subpath: str = "") -> str:
    """Resolve path: if base is absolute use as-is; else resolve relative to PROJECT_ROOT."""
    path = Path(base) if os.path.isabs(base) else (PROJECT_ROOT / base)
    if subpath:
        path = path / subpath
    return str(path)
