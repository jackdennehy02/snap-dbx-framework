# snap-dbx-framework

A YAML-driven ETL framework for Databricks, built as a Databricks-native equivalent of Snap's SETL framework. Implements the medallion architecture with the same layered approach to raw ingest, change detection, surrogate key management, and dimensional loading — designed for Databricks DLT rather than Matillion/Snowflake.

## Architecture

```
Source Files (CSV / cloud storage)
     │
     ▼
[ Bronze — Raw ]        tbl_raw_{object}    Append-only raw ingest. Full source history retained.
     │  Databricks Auto CDC (APPLY CHANGES INTO)
     ▼
[ Bronze — CDC ]        tbl_cdc_{object}    SCD-2 history maintained natively by Databricks.
     │
     ▼
[ Silver — Processed ]  tbl_proc_{object}   Renames Auto CDC columns to __etl_ convention. Typecasting and cleansing.
     │
     ▼
[ Silver — SKEY ]       tbl_skey_{object}   Surrogate key mapping. Business key → integer skey.
     │
     ▼
[ Silver — CONS ]       tbl_cons_{object}   Consolidation. Joins, skey resolution, business rules.
     │
     ▼
[ Gold ]                tbl_dim_{object}    Dimensional model. SCD-2 dimensions and fact tables.
                        tbl_mart_{object}   Aggregations and calculations for reporting.
```

| Layer | SETL Equivalent | Description |
|---|---|---|
| Bronze Raw | STG | Raw ingest from source. Append-only — full ingestion history retained across loads. |
| Bronze CDC | V_STG → PSH | Databricks Auto CDC (`APPLY CHANGES INTO`) applied to raw. Maintains SCD-2 history natively. |
| Silver Processed | _(none)_ | Renames Auto CDC system columns to `__etl_` convention. Typecasting and basic cleansing. |
| Silver SKEY | SKEY | Surrogate key mapping. Insert-only. New skey per effective version for SCD-2 objects. |
| Silver CONS | V_CONS | Consolidation and transformation. Joins, skey resolution, business rule application. |
| Gold Dimensional | DIM / FACT | Final dimensional model. SCD-2 dimensions and fact tables. |
| Gold Data Mart | Data Mart | Aggregation views on top of the dimensional layer. |

## Project Structure

```
snap-dbx-framework/
  config_template/
    objects.yml          # Master object registry template — defines all objects and their layers
    loading.yml          # Bronze raw ingestion config template
    hop.yml              # Silver / gold hop config template
  config/
    objects.yml          # Populated object registry — drives config generation
    loading/             # Generated per-object bronze raw configs
    bronze/
      cdc/               # Generated per-object bronze CDC configs
    silver/
      processed/         # Generated per-object processed configs
      skey/              # Generated per-object SKEY configs
      consolidation/     # Generated per-object CONS configs
    gold/
      dimensional/       # Generated per-object dimension/fact configs
      data_mart/         # Generated per-object data mart configs
  framework/             # Pipeline framework code (in development)
  notebooks/             # Databricks notebooks (in development)
  tests/                 # Test suite (in development)
```

## Config

**`config/objects.yml`** is the master registry. It defines every object, which layers it flows through, the table name at each layer, and whether it's metadata-driven. A generator reads this to produce all per-layer config files.

**Per-layer configs** are generated from `config_template/`. Only fields that differ from `hop_defaults.yml` need to be set — everything else is inherited.

See `architecture.md` for the full config schema and field reference.

## Naming Conventions

All names use `lower_snake_case`.

| Layer | Table Prefix | View Prefix | Example |
|---|---|---|---|
| Bronze Raw | `tbl_raw_` | `vw_raw_` | `tbl_raw_customer` |
| Bronze CDC | `tbl_cdc_` | `vw_cdc_` | `tbl_cdc_customer` |
| Silver Processed | `tbl_proc_` | — | `tbl_proc_customer` |
| Silver SKEY | `tbl_skey_` | — | `tbl_skey_customer` |
| Silver CONS | `tbl_cons_` | — | `tbl_cons_customer` |
| Gold Dimensional | `tbl_dim_` | — | `tbl_dim_customer` |
| Gold Data Mart | `tbl_mart_` | — | `tbl_mart_sales` |

## Audit Columns

| Column | Introduced |
|---|---|
| `__etl_loaded_at` | Bronze Raw |
| `__etl_effective_from` | Silver Processed (renamed from Databricks `__START_AT`) |
| `__etl_effective_to` | Silver Processed (renamed from Databricks `__END_AT`) |
| `__etl_is_current` | Silver Processed (derived from `__END_AT IS NULL`) |

`__etl_record_indicator` is not used — Auto CDC handles change detection internally.
