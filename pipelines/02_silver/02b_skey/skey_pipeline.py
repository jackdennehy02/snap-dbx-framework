# Databricks notebook source
# MAGIC %md
# MAGIC # Silver SKEY Pipeline
# MAGIC Generates surrogate keys for each object using an MD5 hash of the business key fields.

# COMMAND ----------

# DBTITLE 1,Loaders
from pyspark import pipelines as dp
from pyspark.sql import functions as F
import yaml

CATALOG = spark.conf.get("pipeline.catalog", "snap_dbx")
SCHEMA = spark.conf.get("pipeline.schema", "02_silver")
CONFIG_ROOT = spark.conf.get("pipeline.config_root", "/Workspace/Users/jack.dennehy@snapanalytics.co.uk/snap-academy-internal-project/snap-dbx-framework/config")

_FIELD_SEPARATOR = "||"

# COMMAND ----------


def load_objects() -> list[str]:
    """Return object keys that have silver skey enabled in objects.yml."""
    with open(f"{CONFIG_ROOT}/objects.yml", "r") as f:
        registry = yaml.safe_load(f)
    return [
        key for key, obj in registry["objects"].items()
        if obj.get("silver", {}).get("skey", {}).get("enabled", False)
    ]


def load_skey_config(object_key: str) -> dict:
    """Load silver skey config for a given object."""
    with open(f"{CONFIG_ROOT}/02_silver/02b_skey/{object_key}.yml", "r") as f:
        return yaml.safe_load(f)


# ── Column builders ──────────────────────────────────────────────────────────

def _skey_expr(business_key_columns: list[str], scd_type: int) -> F.Column:
    """MD5-based integer surrogate key.

    SCD-2: hashes business key + __etl_effective_from so each version gets a distinct skey.
    SCD-1: hashes business key only — one skey per unique natural key.
    """
    key_cols = business_key_columns + (["__etl_effective_from"] if scd_type == 2 else [])
    concat = F.concat_ws(_FIELD_SEPARATOR, *[F.col(c).cast("string") for c in key_cols])
    return F.conv(F.substring(F.md5(concat), 1, 15), 16, 10).cast("bigint")


def _date_int_expr(col_name: str) -> F.Column:
    """Convert a date column to an integer key in YYYYMMDD format."""
    return F.date_format(F.col(col_name), "yyyyMMdd").cast("int").alias(col_name)


# ── Table registration ───────────────────────────────────────────────────────

def register_skey_table(object_key: str):
    """Register a silver skey table from YAML config."""
    config = load_skey_config(object_key)

    table_name = config["object_name"]
    catalog = config.get("catalog", CATALOG)
    schema = config.get("schema", SCHEMA)
    source_object = config["source_object"]
    scd_type = config.get("scd_type", 1)
    business_key_columns = config["business_key_columns"]
    skey_column = config.get("skey_column", f"{object_key}_skey")
    date_columns = config.get("date_columns") or []

    @dp.table(name=f"{catalog}.{schema}.{table_name}", comment=config.get("comment"))
    def _load():
        df = spark.readStream.table(f"snap_dbx.02_silver.{source_object}")

        if scd_type == 2 and "__etl_effective_from" not in df.columns:
            raise ValueError(
                f"{object_key}: scd_type is 2 but '{source_object}' has no __etl_effective_from. "
                f"This is an SCD-1 object — set scd_type: 1 in the SKEY config."
            )

        # Skey leads; everything from processed follows in its original order.
        # Date columns are converted to YYYYMMDD integers in-place.
        date_col_set = set(date_columns)
        select_cols = [_skey_expr(business_key_columns, scd_type).alias(skey_column)]
        select_cols += [
            _date_int_expr(c) if c in date_col_set else F.col(c)
            for c in df.columns
        ]

        return df.select(select_cols)


# COMMAND ----------

# MAGIC %md
# MAGIC ## SKEY Tables

# COMMAND ----------

# DBTITLE 1,SKEY Tables
for obj in load_objects():
    register_skey_table(obj)
