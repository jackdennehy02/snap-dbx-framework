# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Pipeline
# MAGIC Declarative pipeline for bronze ingestion (raw and CDC).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

CATALOG = spark.conf.get("pipeline.catalog", "snap_dbx")
CONFIG_ROOT = spark.conf.get("pipeline.config_root", "/Workspace/Users/jack.dennehy@snapanalytics.co.uk/snap-academy-internal-project/snap-dbx-framework/config")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

# DBTITLE 1,Cell 7
from pyspark import pipelines as dp
import yaml
from pathlib import Path

# COMMAND ----------

def load_raw_config(object_key: str) -> dict:
    path = Path(CONFIG_ROOT) / "01_bronze" / "01a_raw" / f"{object_key}.yml"
    with open(path) as f:
        return yaml.safe_load(f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loaders

# COMMAND ----------

# DBTITLE 1,Cell 11
def cloud_files_load(config: dict):
    """Load from cloud storage using cloudFiles, driven by the raw config YAML."""
    conn = config.get("connection", {})
    ingestion_mode = config.get("ingestion_mode", "snapshot")

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

    # Use streaming for streaming ingestion, batch for snapshot
    reader = spark.readStream if ingestion_mode == "streaming" else spark.read
    return reader.format("cloudFiles").options(**options).load(
        conn.get("cloud_storage_path", "")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Tables

# COMMAND ----------

dp.table(name="tbl_a_raw_customer")
def raw_customer():
    return cloud_files_load(load_raw_config("customer"))


"""
@dp.table(name="tbl_a_raw_material")
def raw_material():
    return cloud_files_load(load_raw_config("material"))


@dp.table(name="tbl_a_raw_plant")
def raw_plant():
    return cloud_files_load(load_raw_config("plant"))


@dp.table(name="tbl_a_raw_sales")
def raw_sales():
    return cloud_files_load(load_raw_config("sales"))


@dp.table(name="tbl_a_raw_component_bom")
def raw_component_bom():
    return cloud_files_load(load_raw_config("component_bom"))

"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Tables
# MAGIC (to be added)
