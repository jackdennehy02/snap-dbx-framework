# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Processed Pipeline
# MAGIC Applies framework audit column renames, user-defined transforms, and passthrough.

# COMMAND ----------

# DBTITLE 1,Loaders
from pyspark import pipelines as dp
from pyspark.sql import functions as F
import yaml

# ── Fallbacks for pipeline publishing ──
CATALOG = spark.conf.get("pipeline.catalog", "snap_dbx")
SCHEMA = spark.conf.get("pipeline.schema", "02_silver")
CONFIG_ROOT = spark.conf.get("pipeline.config_root", "/Workspace/Users/jack.dennehy@snapanalytics.co.uk/snap-academy-internal-project/snap-dbx-framework/config")

# Source columns consumed by the framework — excluded from passthrough.
_FRAMEWORK_SOURCE_COLS = {"__START_AT", "__END_AT", "__etl_loaded_at"}

# Columns from bronze that should never appear in silver processed.
_DROP_COLS = {"__file_modification_time", "_rescued_data"}

# COMMAND ----------


def load_objects() -> list[str]:
    """Return object keys that have silver processed enabled in objects.yml."""
    with open(f"{CONFIG_ROOT}/objects.yml", "r") as f:
        registry = yaml.safe_load(f)
    return [
        key for key, obj in registry["objects"].items()
        if obj.get("silver", {}).get("processed", {}).get("enabled", False)
    ]


def load_processed_config(object_key: str) -> dict:
    """Load silver processed config for a given object."""
    with open(f"{CONFIG_ROOT}/02_silver/02a_processed/{object_key}.yml", "r") as f:
        return yaml.safe_load(f)


# ── Column builders ─────────────────────────────────────────────────────────

def _framework_transforms() -> list:
    """Standard framework columns — applied to every processed table."""
    return [
        ("__etl_processed_at", F.current_timestamp()),
        ("__etl_active_from",  F.col("__START_AT")),
        ("__etl_active_to",
            F.when(F.col("__END_AT").isNull(), F.lit("9999-12-31 23:59:59").cast("timestamp"))
             .otherwise(F.col("__END_AT"))),
        ("__etl_is_current",   (F.col("__END_AT").isNull()).cast("boolean")),
    ]


def _user_transforms(columns_config: list) -> list:
    """Build (name, expr) pairs from the YAML columns config."""
    result = []
    for col in (columns_config or []):
        target = col.get("target_col")
        if not target:
            continue
        source = col.get("source_col")
        expr = col.get("expression")
        data_type = col.get("data_type")
        if expr and data_type:
            result.append((target, F.expr(expr).cast(data_type)))
        elif expr:
            result.append((target, F.expr(expr)))
        elif source and data_type:
            result.append((target, F.col(source).cast(data_type)))
        elif source:
            result.append((target, F.col(source)))
    return result


def _passthrough_cols(source_cols: list, skip: set) -> list:
    """Return (name, expr) pairs for source columns not explicitly handled."""
    return [(c, F.col(c)) for c in source_cols if c not in skip]


def _apply_leading_columns(col_pairs: list, leading: list) -> list:
    """Reorder col_pairs so leading columns appear first."""
    if not leading:
        return col_pairs
    col_map = dict(col_pairs)
    leading_pairs = [(c, col_map[c]) for c in leading if c in col_map]
    rest = [(n, e) for n, e in col_pairs if n not in set(leading)]
    return leading_pairs + rest


# ── Table registration ───────────────────────────────────────────────────────

def register_processed_table(object_key: str):
    """Register a silver processed table from YAML config."""
    config = load_processed_config(object_key)

    table_name = config["object_name"]
    catalog = config.get("catalog", CATALOG)
    schema = config.get("schema", SCHEMA)
    source_object = config["source_object"]
    columns_config = config.get("columns") or []
    leading_columns = config.get("leading_columns") or []

    user_source_cols = {c["source_col"] for c in columns_config if c.get("source_col")}
    skip = _FRAMEWORK_SOURCE_COLS | _DROP_COLS | user_source_cols

    @dp.table(name=f"{catalog}.{schema}.{table_name}", comment=config.get("comment"))
    def _load():
        df = spark.readStream.table(f"snap_dbx.01_bronze.{source_object}")

        framework = _framework_transforms()
        user = _user_transforms(columns_config)
        passthrough = _passthrough_cols(df.columns, skip)

        col_pairs = _apply_leading_columns(passthrough + user + framework, leading_columns)

        return df.select(*[expr.alias(name) for name, expr in col_pairs])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Processed Tables

# COMMAND ----------

# DBTITLE 1,Processed Tables
for obj in ("customer", "material", "plant"):
#load_objects()
    register_processed_table(obj)


