# Databricks notebook source
# MAGIC %md
# MAGIC # Drop All Objects
# MAGIC Drops every table/view registered across all pipeline layers for all objects in objects.yml.
# MAGIC Run this to fully reset the environment before a clean rebuild.

# COMMAND ----------

# DBTITLE 1,Cell 2
import yaml

CONFIG_ROOT = "/Workspace/Users/jack.dennehy@snapanalytics.co.uk/snap-academy-internal-project/snap-dbx-framework/config"

CATALOG = "snap_dbx"

with open(f"{CONFIG_ROOT}/objects.yml", "r") as f:
    registry = yaml.safe_load(f)

objects = registry.get("objects", {})

# Collect every object_name defined across all layers
tables = []
for obj_config in objects.values():
    if not obj_config:
        continue
    for layer_config in (obj_config.get(l) for l in ("bronze", "silver", "gold")):
        if not layer_config:
            continue
        for hop_config in layer_config.values():
            if not hop_config:
                continue
            name = hop_config.get("object_name")
            if name:
                schema = {
                    "01_bronze": "01_bronze",
                    "02_silver": "02_silver",
                    "03_gold":   "03_gold",
                }.get(
                    next((s for s in ("01_bronze", "02_silver", "03_gold") if any(
                        name.startswith(p) for p in (
                            "tbl_a_raw_", "tbl_b_cdc_",
                            "tbl_a_proc_", "tbl_b_skey_", "tbl_c_cons_",
                            "tbl_a_dim_", "tbl_a_fact_", "tbl_b_mart_",
                        )
                    )), None),
                    None
                )
                # Derive schema from table prefix
                if name.startswith(("tbl_a_raw_", "tbl_b_cdc_")):
                    schema = "01_bronze"
                elif name.startswith(("tbl_a_proc_", "tbl_b_skey_", "tbl_c_cons_")):
                    schema = "02_silver"
                elif name.startswith(("tbl_a_dim_", "tbl_a_fact_", "tbl_b_mart_")):
                    schema = "03_gold"
                else:
                    schema = None

                if schema:
                    tables.append(f"{CATALOG}.{schema}.{name}")

tables = sorted(set(tables))

print(f"Dropping {len(tables)} tables from catalog '{CATALOG}':\n")
for t in tables:
    print(f"  {t}")

# COMMAND ----------

dropped = []
failed = []

for table in tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"  dropped  {table}")
        dropped.append(table)
    except Exception as e:
        print(f"  FAILED   {table} — {e}")
        failed.append(table)

print(f"\nDone. Dropped: {len(dropped)}  Failed: {len(failed)}")
