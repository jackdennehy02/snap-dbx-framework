# snap-dbx-framework

A YAML-driven ETL framework for Databricks, built as a Databricks-native equivalent of Snap's SETL framework. Implements the medallion architecture with the same layered approach to raw ingest, change detection, surrogate key management, and dimensional loading — designed for Databricks DLT rather than Matillion/Snowflake.

## Architecture

```
Source Files (CSV / cloud storage)
     │
     ▼
[ Bronze — Raw ]        raw_{object}         Append-only raw ingest. Full source history retained.
     │  Databricks Auto CDC (APPLY CHANGES INTO)
     ▼
[ Bronze — CDC ]        cdc_{object}         SCD-2 history maintained natively by Databricks.
     │
     ▼
[ Silver — Processed ]  processed_{object}   Renames Auto CDC columns to __etl_ convention. Typecasting and cleansing.
     │
     ▼
[ Silver — SKEY ]       skey_{object}        Surrogate key mapping. Business key → integer skey.
     │
     ▼
[ Gold — CONS ]         cons_{object}        Consolidation. Joins processed + SKEY, business rules.
     │
     ▼
[ Gold — Dimensional ]  dim_{object}         Dimensional model. SCD-2 dimensions and fact tables.
                        fact_{object}
```

| Layer | SETL Equivalent | Description |
|---|---|---|
| Bronze Raw | STG | Raw ingest from source. Append-only — full ingestion history retained across loads. |
| Bronze CDC | V_STG → PSH | Databricks Auto CDC (`APPLY CHANGES INTO`) applied to `raw_`. Maintains SCD-2 history natively. |
| Silver Processed | _(none)_ | Renames Auto CDC system columns to `__etl_` convention. Typecasting and basic cleansing. |
| Silver SKEY | SKEY | Surrogate key mapping. Insert-only. New skey per effective version for SCD-2 objects. |
| Gold CONS | V_CONS | Consolidation and transformation. Joins processed + SKEY, skey resolution, business rule application. |
| Gold Dimensional | DIM / FACT | Final dimensional model. SCD-2 dimensions and fact tables. |

## Project Structure

```
snap-dbx-framework/
  config/
    framework.yml          # Centralised catalogs, schemas, and environment variables
    objects.yml            # Master object registry — drives config file generation
    01_bronze/
      raw/                 # Generated per-object bronze raw configs
      cdc/                 # Generated per-object bronze CDC configs
    02_silver/
      processed/           # Generated per-object processed configs
      skey/                # Generated per-object SKEY configs
    03_gold/
      consolidation/       # Generated per-object CONS configs
      dimensional/         # Generated per-object dimension/fact configs
  config_templates/        # Template files used by the generator
  framework/
    YAML_Generator.py      # Generates per-layer config files from objects.yml
  pipelines/
    01_bronze/
      raw/                 # Bronze raw pipeline
      cdc/                 # Bronze CDC pipeline
    02_silver/
      processed/           # Silver processed pipeline
      skey/                # Silver SKEY pipeline
    03_gold/               # Gold pipelines (in development)
  architecture.md          # Full architecture reference
```

## Config

**`config/framework.yml`** is the single source of truth for catalog names, schema names, and pipeline environment variables. All pipelines load from it at runtime — nothing is hardcoded in pipeline code.

**`config/objects.yml`** is the master object registry. It defines every object, which layers it flows through, and whether it's metadata-driven. The generator reads it to produce all per-layer config files.

See `architecture.md` for the full config schema and field reference.

## Naming Conventions

All names use `lower_snake_case`. No `tbl_` prefix.

| Layer | Prefix | Example |
|---|---|---|
| Bronze Raw | `raw_` | `raw_customer` |
| Bronze CDC | `cdc_` | `cdc_customer` |
| Silver Processed | `processed_` | `processed_customer` |
| Silver SKEY | `skey_` | `skey_customer` |
| Gold CONS | `cons_` | `cons_customer` |
| Gold Dimensional | `dim_` / `fact_` | `dim_customer`, `fact_sales` |

## Audit Columns

| Column | Introduced | Notes |
|---|---|---|
| `__etl_loaded_at` | Bronze Raw | When the record was loaded |
| `__source_updated_at` | Bronze Raw | Source file modification time |
| `__etl_processed_at` | Silver Processed | When the record passed through processed |
| `__etl_effective_from` | Silver Processed | Renamed from Databricks `__START_AT` (SCD-2 only) |
| `__etl_effective_to` | Silver Processed | Renamed from `__END_AT`; `NULL` replaced with `ev_end_date` (SCD-2 only) |
| `__etl_is_current` | Silver Processed | Derived from `__END_AT IS NULL` (SCD-2 only) |

SCD-1 objects have no versioning — effective date columns are omitted entirely rather than populated with placeholder values.
