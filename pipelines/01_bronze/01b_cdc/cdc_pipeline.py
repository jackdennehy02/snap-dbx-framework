# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze CDC Pipeline
# MAGIC Declarative pipeline for bronze CDC (APPLY CHANGES INTO)

# COMMAND ----------

# DBTITLE 1,Loaders
from pyspark import pipelines as dp
from pyspark.sql import functions as F, Window
import yaml

# ── Fallbacks for pipeline publishing ──
CATALOG = spark.conf.get("pipeline.catalog", "snap_dbx")
SCHEMA = spark.conf.get("pipeline.schema", "01_bronze")
CONFIG_ROOT = spark.conf.get("pipeline.config_root", "/Workspace/Users/jack.dennehy@snapanalytics.co.uk/snap-academy-internal-project/snap-dbx-framework/config")

# Framework audit columns added at raw ingest — never source data, never tracked.
_ETL_AUDIT_COLUMNS = ["_etl_loaded_at", "_metadata_file_modification_time"]

# COMMAND ----------


def load_objects() -> list[str]:
    """Return object keys that have bronze CDC enabled in objects.yml."""
    with open(f"{CONFIG_ROOT}/objects.yml", "r") as f:
        registry = yaml.safe_load(f)
    return [
        key for key, obj in registry["objects"].items()
        if obj.get("bronze", {}).get("cdc", {}).get("enabled", False)
    ]


def load_cdc_config(object_key: str) -> dict:
    """Load CDC layer config for a given object."""
    config_path = f"{CONFIG_ROOT}/01_bronze/01b_cdc/{object_key}.yml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def register_cdc_table(object_key: str):
    """Register a CDC table from YAML config — tracked (APPLY CHANGES INTO) or untracked (full overwrite)."""
    config = load_cdc_config(object_key)

    table_name = config["object_name"]
    catalog = config.get("catalog", CATALOG)
    schema = config.get("schema", SCHEMA)
    source_object = config["source_object"]
    track_changes = config.get("track_changes", True)

    keys = config.get("keys") or []

    if not keys:
        raise ValueError(f"{object_key}: 'keys' is required for CDC.")

    if not track_changes:
        @dp.table(
            name=f"{catalog}.{schema}.{table_name}",
            comment=config.get("comment"),
        )
        def _load():
            w = Window.partitionBy(*keys).orderBy(F.col("_etl_loaded_at").desc())
            return (
                spark.read.table(f"{catalog}.{schema}.{source_object}")
                .withColumn("_rn", F.row_number().over(w))
                .filter(F.col("_rn") == 1)
                .drop("_rn")
            )
        return

    sequence_by = config.get("sequence_by")

    if not keys:
        raise ValueError(f"{object_key}: 'keys' is required for CDC.")
    if not sequence_by:
        raise ValueError(f"{object_key}: 'sequence_by' is required for CDC.")

    full_table_name = f"{catalog}.{schema}.{table_name}"

    # create_streaming_table is idempotent — no-ops if the table already exists.
    # Required before apply_changes so DLT has a target to write into on first run.
    dp.create_streaming_table(
        name=full_table_name,
        comment=config.get("comment"),
    )

    kwargs = dict(
        target=full_table_name,
        source=spark.readStream.table(f"{catalog}.{schema}.{source_object}"),
        keys=keys,
        sequence_by=sequence_by,
        stored_as_scd_type=config.get("scd_type", 2),
        ignore_null_updates=config.get("ignore_null_updates", False),
        except_column_list=_ETL_AUDIT_COLUMNS,
    )

    if config.get("apply_as_deletes"):
        kwargs["apply_as_deletes"] = config["apply_as_deletes"]
    if config.get("track_history_column_list"):
        kwargs["track_history_column_list"] = config["track_history_column_list"]
    if config.get("comment"):
        kwargs["comment"] = config["comment"]

    dp.apply_changes(**kwargs)


# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Tables

# COMMAND ----------

# DBTITLE 1,CDC Tables
##for obj in load_objects():
register_cdc_table("customer")
