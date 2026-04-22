# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Raw Pipeline
# MAGIC Declarative pipeline for bronze ingestion (raw)

# COMMAND ----------

# DBTITLE 1,Loaders
from pyspark import pipelines as dp
from pyspark.sql import functions as F
import yaml

CONFIG_ROOT = spark.conf.get("pipeline.ev_config_root")
CATALOG     = spark.conf.get("pipeline.catalog_bronze")
SCHEMA      = spark.conf.get("pipeline.schema_bronze")



def load_objects() -> list[str]:
    """Return object keys that have bronze raw enabled in objects.yml."""
    with open(f"{CONFIG_ROOT}/objects.yml", "r") as f:
        registry = yaml.safe_load(f)
    return [
        key for key, obj in registry["objects"].items()
        if obj.get("bronze", {}).get("raw", {}).get("enabled", False)
    ]


def load_raw_config(object_key: str) -> dict:
    """Load raw layer config for a given object."""
    config_path = f"{CONFIG_ROOT}/01_bronze/raw/{object_key}.yml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ── Source reader builders ──────────────────────────────────────────────────
# Each accepts (conn: dict, streaming: bool) and returns a DataFrame.
# Register new source types in _SOURCE_READERS below.

def _cloud_storage_reader(conn: dict, streaming: bool):
    file_format = conn.get("cloud_storage_type", "")
    path = conn.get("cloud_storage_path", "")

    if not streaming:
        # cloudFiles is streaming-only — batch reads use the underlying format directly
        options = {}
        if conn.get("header") is not None:
            options["header"] = str(conn["header"]).lower()
        if conn.get("delimiter"):
            options["delimiter"] = conn["delimiter"]
        if conn.get("null_value"):
            options["nullValue"] = conn["null_value"]
        return spark.read.format(file_format).options(**options).load(path)

    options = {"cloudFiles.format": file_format}
    if conn.get("header") is not None:
        options["header"] = str(conn["header"]).lower()
    if conn.get("delimiter"):
        options["delimiter"] = conn["delimiter"]
    if conn.get("null_value"):
        options["nullValue"] = conn["null_value"]
    if conn.get("cloud_schema_location"):
        options["cloudFiles.schemaLocation"] = conn["cloud_schema_location"]
    if conn.get("cloud_ingestion_mode"):
        options["cloudFiles.useIncrementalListing"] = (
            "false" if conn["cloud_ingestion_mode"] == "directory_listing" else "true"
        )
    return spark.readStream.format("cloudFiles").options(**options).load(path)


_SOURCE_READERS = {
    "cloud_storage": _cloud_storage_reader,
    # "jdbc": _jdbc_reader,
    # "kafka": _kafka_reader,
}


# ── Validation ──────────────────────────────────────────────────────────────

_INVALID_COMBOS = {
    # Raw is append-only — upsert belongs in the CDC layer.
    ("streaming", "upsert"): "upsert is not valid in the raw layer; raw is append-only.",
    ("snapshot", "upsert"): "upsert is not valid in the raw layer; raw is append-only.",
    # Streaming tables don't support overwrite — DLT appends continuously.
    ("streaming", "overwrite"): "overwrite is incompatible with streaming ingestion.",
}


def _validate_raw_config(object_key: str, ingestion_mode: str, merge_strategy: str):
    reason = _INVALID_COMBOS.get((ingestion_mode, merge_strategy))
    if reason:
        raise ValueError(f"{object_key}: {reason}")


# ── Table registration ──────────────────────────────────────────────────────

def register_raw_table(object_key: str):
    """Register a dp.table for a raw source object based on its YAML config."""
    config = load_raw_config(object_key)
    source_type = config.get("source_type", "cloud_storage")
    conn = config.get("connection", {})
    ingestion_mode = config.get(
        "ingestion_mode",
        spark.conf.get("pipeline.ingestion_mode", "snapshot"),
    )
    merge_strategy = config.get("merge_strategy", "append")

    table_name = config["object_name"]
    catalog = config.get("catalog", CATALOG)
    schema = config.get("schema", SCHEMA)
    comment = config.get("comment")
    data_items = config.get("data_items")

    _validate_raw_config(object_key, ingestion_mode, merge_strategy)

    reader_fn = _SOURCE_READERS.get(source_type)
    if reader_fn is None:
        raise ValueError(f"{object_key}: unknown source_type '{source_type}'")

    streaming = ingestion_mode == "streaming"

    @dp.table(name=f"{catalog}.{schema}.{table_name}", comment=comment)
    def _load():
        df = reader_fn(conn, streaming)

        if data_items:
            df = df.selectExpr(*data_items)

        return (
            df
            .withColumn("__source_updated_at",
                        F.col("_metadata.file_modification_time"))
            .withColumn("__etl_loaded_at", F.current_timestamp())
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Tables

# COMMAND ----------

# DBTITLE 1,Raw Tables
for obj in load_objects():
    register_raw_table(obj)
