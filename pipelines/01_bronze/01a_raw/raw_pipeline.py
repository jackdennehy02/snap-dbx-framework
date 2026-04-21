# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Raw Pipeline
# MAGIC Declarative pipeline for bronze ingestion (raw)

# COMMAND ----------

# DBTITLE 1,Loaders
from pyspark import pipelines as dp
from pyspark.sql import functions as F
import yaml

# ── Fallbacks for pipeline publishing ──
CATALOG = spark.conf.get("pipeline.catalog", "snap_dbx")
SCHEMA = spark.conf.get("pipeline.schema", "01_bronze")



def load_raw_config(object_key: str) -> dict:
    """Load raw layer config for a given object."""
    config_path = f"{CONFIG_ROOT}/01_bronze/01a_raw/{object_key}.yml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def register_cloud_files_raw_table(object_key: str):
    """Load YAML config and register a dp.table for a raw cloud_files source."""
    config = load_raw_config(object_key)
    conn = config.get("connection", {})
    ingestion_mode = config.get("ingestion_mode",
                                spark.conf.get("pipeline.ingestion_mode", "snapshot"))

    # ── Target ──────────────────────────────────────
    table_name = config["object_name"]
    catalog = config.get("catalog", CATALOG)
    schema = config.get("schema", SCHEMA)
    comment = config.get("comment")
    data_items = config.get("data_items")

    # ── cloudFiles options ──────────────────────────
    options = {
        "cloudFiles.format": conn.get("cloud_storage_type", ""),
    }
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

    # ── Register the table ──────────────────────────
    @dp.table(
        name=f"{catalog}.{schema}.{table_name}",
        comment=comment,
    )
    def _load():
        reader = spark.readStream if ingestion_mode == "streaming" else spark.read
        df = reader.format("cloudFiles").options(**options).load(
            conn.get("cloud_storage_path", "")
        )

        if data_items:
            df = df.selectExpr(*data_items)

        df = (
            df
            .withColumn("_metadata_file_modification_time",
                         F.col("_metadata.file_modification_time"))
            .withColumn("_etl_loaded_at", F.current_timestamp())
        )

        return df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Tables

# COMMAND ----------

# DBTITLE 1,Raw Tables
for obj in ["customer", "material", "plant", "sales", "component_bom" ]:
    register_cloud_files_raw_table(obj)
