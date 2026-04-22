# Databricks notebook source
# MAGIC %md
# MAGIC # Silver SKEY Pipeline
# MAGIC Generates surrogate keys for each object using an MD5 hash of the business key fields.

# COMMAND ----------

# DBTITLE 1,Loaders
# MAGIC %run ../../tools/utils

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CONFIG_ROOT       = spark.conf.get("ev_config_root")
CATALOG           = spark.conf.get("catalog_skey")
SCHEMA            = spark.conf.get("schema_skey")
PROCESSED_CATALOG = spark.conf.get("catalog_processed")
PROCESSED_SCHEMA  = spark.conf.get("schema_processed")
_FIELD_SEPARATOR  = spark.conf.get("ev_field_separator")


# ── Column builders ──────────────────────────────────────────────────────────

def _skey_expr(business_key_columns: list[str], scd_type: int) -> F.Column:
    """MD5-based integer surrogate key.

    SCD-2: hashes business key + __etl_effective_from so each version gets a distinct skey.
    SCD-1: hashes business key only — one skey per unique natural key.
    """
    key_cols = business_key_columns + (["__etl_effective_from"] if scd_type == 2 else [])
    concat = F.concat_ws(_FIELD_SEPARATOR, *[F.col(c).cast("string") for c in key_cols])
    return F.conv(F.substring(F.md5(concat), 1, 15), 16, 10).cast("bigint")


# ── Table registration ───────────────────────────────────────────────────────

def register_skey_table(object_key: str):
    """Register a silver skey table from YAML config."""
    config = load_hop_config(CONFIG_ROOT, "02_silver/skey", object_key)

    table_name = config["object_name"]
    catalog = config.get("catalog", CATALOG)
    schema = config.get("schema", SCHEMA)
    source_object = config["source_object"]
    scd_type = config.get("scd_type", 1)
    business_key_columns = config["business_key_columns"]
    skey_column = config.get("skey_column", f"{object_key}_skey")

    @dp.table(name=f"{catalog}.{schema}.{table_name}", comment=config.get("comment"))
    def _load():
        df = spark.readStream.table(f"{PROCESSED_CATALOG}.{PROCESSED_SCHEMA}.{source_object}")

        if scd_type == 2 and "__etl_effective_from" not in df.columns:
            raise ValueError(
                f"{object_key}: scd_type is 2 but '{source_object}' has no __etl_effective_from. "
                f"This is an SCD-1 object — set scd_type: 1 in the SKEY config."
            )

        # Key mapping only: skey → business keys → effectivity (SCD-2) → date keys → audit.
        # CONS reads from processed for business attributes and joins here for skeys.
        select_cols = [_skey_expr(business_key_columns, scd_type).alias(skey_column)]
        select_cols += [F.col(c) for c in business_key_columns]
        if scd_type == 2:
            select_cols.append(F.col("__etl_effective_from"))
            select_cols.append(F.col("__etl_effective_to"))

        return df.select(select_cols)


# COMMAND ----------

# MAGIC %md
# MAGIC ## SKEY Tables

# COMMAND ----------

# DBTITLE 1,SKEY Tables
for obj in load_objects(CONFIG_ROOT, "silver", "skey"):
    register_skey_table(obj)
