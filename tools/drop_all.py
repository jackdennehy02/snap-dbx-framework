# Databricks notebook source
# MAGIC %md
# MAGIC # Drop Objects
# MAGIC Drops pipeline tables for the selected layers. Run before a clean rebuild.

# COMMAND ----------

import yaml

dbutils.widgets.text("config_root", "", "ev_config_root — path to the config directory")
dbutils.widgets.text("catalog", "snap_dbx", "Catalog")
dbutils.widgets.dropdown("layers", "all", ["all", "bronze", "silver", "gold"], "Layers to drop")

CONFIG_ROOT = dbutils.widgets.get("config_root")
CATALOG     = dbutils.widgets.get("catalog")
LAYERS      = dbutils.widgets.get("layers")

LAYER_SCHEMAS = {
    "bronze": "01_bronze",
    "silver": "02_silver",
    "gold":   "03_gold",
}

active_layers = set(LAYER_SCHEMAS) if LAYERS == "all" else {LAYERS}

with open(f"{CONFIG_ROOT}/objects.yml") as f:
    registry = yaml.safe_load(f)

tables = []
for obj_config in registry.get("objects", {}).values():
    if not obj_config:
        continue
    for layer, layer_config in obj_config.items():
        if layer not in active_layers or not layer_config:
            continue
        for hop_config in layer_config.values():
            if not hop_config:
                continue
            name = hop_config.get("object_name")
            if name:
                tables.append(f"{CATALOG}.{LAYER_SCHEMAS[layer]}.{name}")

tables = sorted(set(tables))

print(f"Dropping {len(tables)} tables from '{CATALOG}' (layers: {LAYERS}):\n")
for t in tables:
    print(f"  {t}")

# COMMAND ----------

dropped = []
failed  = []

for table in tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"  dropped  {table}")
        dropped.append(table)
    except Exception as e:
        print(f"  FAILED   {table} — {e}")
        failed.append(table)

print(f"\nDone. Dropped: {len(dropped)}  Failed: {len(failed)}")
