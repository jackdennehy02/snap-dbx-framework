# Databricks notebook source
# MAGIC %md
# MAGIC # Silver CDC Pipeline
# MAGIC Declarative pipeline for silver CDC (APPLY CHANGES INTO)

# COMMAND ----------

# DBTITLE 1,Loaders
import sys
sys.path.insert(0, '..')
from utils import load_objects, load_hop_config, parse_timestamp_robust

from pyspark import pipelines as dp
from pyspark.sql import functions as F

CONFIG_ROOT  = spark.conf.get("ev_config_root")
L01_CATALOG  = spark.conf.get("ev_l01_catalog")
L01_SCHEMA   = spark.conf.get("ev_l01_schema")
L02_LOCATION = spark.conf.get("ev_l02_location")
L02_NAME     = spark.conf.get("ev_l02_name")
CATALOG      = spark.conf.get("ev_l02_catalog")
SCHEMA       = spark.conf.get("ev_l02_schema")

# Columns excluded from CDC target — Auto Loader internals only.
# __etl_loaded_at and __source_updated_time pass through to silver.
_CDC_EXCLUDED_COLUMNS = ["_rescued_data"]

# COMMAND ----------


def register_cdc_table(object_key: str):
    """Register a CDC table from YAML config using Auto CDC."""
    config = load_hop_config(CONFIG_ROOT, L02_LOCATION, L02_NAME, object_key)

    table_name = config["object_name"]
    catalog = config.get("catalog", CATALOG)
    schema = config.get("schema", SCHEMA)
    source_object = config["source_object"]

    keys = config.get("keys") or []
    sequence_by = config.get("sequence_by")

    if not keys:
        raise ValueError(f"{object_key}: 'keys' is required for CDC.")
    if not sequence_by:
        raise ValueError(f"{object_key}: 'sequence_by' is required for CDC.")

    full_table_name = f"{catalog}.{schema}.{table_name}"
    source_table = f"{L01_CATALOG}.{L01_SCHEMA}.{source_object}"

    # create_streaming_table is idempotent — no-ops if the table already exists.
    # Required before create_auto_cdc_flow so DLT has a target on first run.
    dp.create_streaming_table(
        name=full_table_name,
        comment=config.get("comment"),
    )

    kwargs = dict(
        target=full_table_name,
        source=source_table,
        keys=keys,
        sequence_by=parse_timestamp_robust(F.col(sequence_by)),
        stored_as_scd_type=config.get("scd_type", 2),
        ignore_null_updates=config.get("ignore_null_updates", False),
        except_column_list=_CDC_EXCLUDED_COLUMNS,
    )

    if config.get("apply_as_deletes"):
        kwargs["apply_as_deletes"] = config["apply_as_deletes"]
    if config.get("track_history_column_list"):
        kwargs["track_history_column_list"] = config["track_history_column_list"]

    dp.create_auto_cdc_flow(**kwargs)


# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Tables

# COMMAND ----------

# DBTITLE 1,CDC Tables
for obj in load_objects(CONFIG_ROOT, "l02"):
    register_cdc_table(obj)
