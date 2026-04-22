# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Processed Pipeline
# MAGIC Applies framework audit column renames, user-defined transforms, and passthrough.

# COMMAND ----------

# DBTITLE 1,Loaders
# MAGIC %run ../../tools/utils

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CONFIG_ROOT    = spark.conf.get("ev_config_root")
CATALOG        = spark.conf.get("catalog_silver")
SCHEMA         = spark.conf.get("schema_silver")
BRONZE_CATALOG = spark.conf.get("catalog_bronze")
BRONZE_SCHEMA  = spark.conf.get("schema_bronze")
_EV_END_DATE   = spark.conf.get("ev_end_date")

# Source columns consumed by the framework — excluded from passthrough.
_FRAMEWORK_SOURCE_COLS = {"__START_AT", "__END_AT", "__etl_loaded_at", "__source_updated_at"}

# Columns from bronze that should never appear in silver processed.
_DROP_COLS = {"_rescued_data"}

# COMMAND ----------




# ── Column builders ─────────────────────────────────────────────────────────

def _framework_transforms(source_cols: list) -> list:
    """Standard framework columns — applied to every processed table.

    SCD-2 sources have __START_AT/__END_AT from Auto CDC; effective date columns
    are derived from these and included in the output.
    SCD-1 sources have no versioning — effective date columns are omitted entirely.
    """
    is_scd2 = "__START_AT" in source_cols
    end_of_time = F.lit(_EV_END_DATE).cast("timestamp")

    cols = [
        ("__source_updated_at", F.col("__source_updated_at")),
        ("__etl_loaded_at",     F.col("__etl_loaded_at")),
        ("__etl_processed_at",  F.current_timestamp()),
    ]

    if is_scd2:
        cols += [
            ("__etl_effective_from", F.col("__START_AT")),
            ("__etl_effective_to",
                F.when(F.col("__END_AT").isNull(), end_of_time).otherwise(F.col("__END_AT"))),
            ("__etl_is_current",
                (F.col("__END_AT").isNull()).cast("boolean")),
        ]

    return cols


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
    config = load_hop_config(CONFIG_ROOT, "02_silver/processed", object_key)

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
        df = spark.readStream.table(f"{BRONZE_CATALOG}.{BRONZE_SCHEMA}.{source_object}")

        framework = _framework_transforms(df.columns)
        user = _user_transforms(columns_config)
        passthrough = _passthrough_cols(df.columns, skip)

        col_pairs = _apply_leading_columns(passthrough + user + framework, leading_columns)

        return df.select(*[expr.alias(name) for name, expr in col_pairs])


# COMMAND ----------

# MAGIC %md
# MAGIC ## Processed Tables

# COMMAND ----------

# DBTITLE 1,Processed Tables
for obj in load_objects(CONFIG_ROOT, "silver", "processed"):
    register_processed_table(obj)


