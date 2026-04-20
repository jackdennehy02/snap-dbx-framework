# snap-dbx-framework

A YAML-driven ETL framework for Databricks, built as a Databricks-native equivalent of Snap's SETL framework (Matillion/Snowflake). Implements the medallion architecture and mirrors SETL's layered approach to staging, change detection, surrogate key management, and dimensional loading.

## Architecture

```
Bronze  →  Silver (CDC + SKEY)  →  Gold (Dimensions + Facts)
  ↑
CSV source files (uploaded to Databricks Volume)
```

| Layer | SETL Equivalent | Description |
|---|---|---|
| Bronze | STG | Raw ingest from source CSVs. Metadata columns appended. Overwritten each load. |
| Silver CDC | PSH | Change detection (N/I/C/D) against prior load. Append-only history table. |
| Silver SKEY | SKEY | Surrogate key mapping. Business key → integer skey. Insert-only. |
| Silver CONS | V_CONS | Consolidation / transformation logic. Joins sources, applies business rules. |
| Gold | DIM / FACT | Final dimensional model. SCD-2 dimensions and fact table. |

## Project Structure

```
snap-dbx-framework/
  config/
    pipeline.yml              # Top-level pipeline config (run order, settings)
    silver_transforms.yml     # Shared library of reusable silver-layer transforms
    bronze/                   # Per-entity bronze ingestion config
      customer.yml            #   source file pattern, encoding, cast overrides, filter
      material.yml
      plant.yml
      sales.yml
      component_bom.yml
    silver/                   # Per-entity silver config (CDC + SKEY + CONS)
      customer.yml            #   keys, scd_type, tracked fields, transforms, consolidation
      material.yml
      plant.yml
      sales.yml
      component_bom.yml
    gold/                     # Per-entity gold config
      customer.yml            #   scd_type, target table, partition columns
      material.yml
      plant.yml
      sales.yml
      component_bom.yml
  notebooks/
    01_bronze_ingest.py       # Load CSVs to bronze Delta tables
    02_silver_cdc.py          # CDC detection, write to silver history tables
    03_silver_skey.py         # Generate and maintain surrogate keys
    04_silver_consolidation.py # Transformation logic, joins, business rules
    05_gold_dimensions.py     # SCD-2 merge into dimension tables
    06_gold_facts.py          # Fact table load
  framework/
    loader.py                 # Reads YAML config, drives pipeline execution
    cdc.py                    # N/I/C/D detection logic
    skey.py                   # Surrogate key generation and lookup
    scd2.py                   # SCD-2 expire + insert logic
    metadata.py               # Metadata column helpers (etl_load_timestamp etc.)
  tests/
    test_cdc.py
    test_skey.py
    test_scd2.py
```

## YAML Config

Each entity has three config files — one per hop. The framework loads the relevant directory for each notebook stage, so each hop only sees what it needs.

| Hop | Directory | Key settings |
|---|---|---|
| Bronze | `config/bronze/` | `source` (file pattern, encoding, delimiter), `filter`, `column_map`, `cast_overrides` |
| Silver | `config/silver/` | `keys`, `scd_type`, `tracked_fields`, `silver_transforms`, `consolidation` |
| Gold | `config/gold/` | `scd_type`, `targets.table`, `partition_by` |

Reusable silver transforms are defined in `config/silver_transforms.yml` and referenced by name in each entity's silver config. The `{col}` placeholder in `expr` is substituted at runtime with the target column name.

## Metadata Columns

All tables carry standard metadata columns, consistent with SETL conventions:

| Column | Description |
|---|---|
| `etl_load_timestamp` | When the record was loaded |
| `etl_source_id` | Source system identifier |
| `etl_record_indicator` | N / I / C / D |
| `etl_effective_from` | Start of validity window |
| `etl_effective_to` | End of validity window (`9999-12-31` if current) |
| `etl_is_current` | Boolean flag for active records |
| `etl_fprint` | MD5 fingerprint of tracked fields |

## Naming Conventions

Databricks convention: `lower_snake_case` for all table and column names.

| Layer | Prefix | Example |
|---|---|---|
| Bronze | `bronze_` | `bronze_customer` |
| Silver CDC | `silver_` | `silver_customer` |
| Silver SKEY | `skey_` | `skey_customer` |
| Gold Dim | `dim_` | `dim_customer` |
| Gold Fact | `fact_` | `fact_sales` |
