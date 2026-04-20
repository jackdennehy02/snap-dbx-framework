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

import sys
sys.path.insert(0, "../framework")

from unity_catalog_manager import UnityManager

uc = UnityManager(spark, catalog=CATALOG)
uc.create_container("CATALOG", CATALOG)
uc.create_container("SCHEMA", "01_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports

# COMMAND ----------

import dlt
import yaml
from pathlib import Path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Config

# COMMAND ----------

def load_raw_config(object_key: str) -> dict:
    path = Path(CONFIG_ROOT) / "01_bronze" / "01a_raw" / f"{object_key}.yml"
    with open(path) as f:
        return yaml.safe_load(f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loaders

# COMMAND ----------

def cloud_files_load(config: dict):
    """Load from cloud storage using cloudFiles, driven by the raw config YAML."""
    conn = config.get("connection", {})

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

    return spark.read.format("cloudFiles").options(**options).load(
        conn.get("cloud_storage_path", "")
    )


# def jdbc_load(config: dict): ...
# def kafka_load(config: dict): ...

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raw Tables

# COMMAND ----------

@dlt.table(name="tbl_a_raw_customer")
def raw_customer():
    return cloud_files_load(load_raw_config("customer"))


@dlt.table(name="tbl_a_raw_material")
def raw_material():
    return cloud_files_load(load_raw_config("material"))


@dlt.table(name="tbl_a_raw_plant")
def raw_plant():
    return cloud_files_load(load_raw_config("plant"))


@dlt.table(name="tbl_a_raw_sales")
def raw_sales():
    return cloud_files_load(load_raw_config("sales"))


@dlt.table(name="tbl_a_raw_component_bom")
def raw_component_bom():
    return cloud_files_load(load_raw_config("component_bom"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## CDC Tables
# MAGIC (to be added)
