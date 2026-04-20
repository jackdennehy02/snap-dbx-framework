# snap-dbx-framework

A YAML-driven ETL framework for Databricks, built as a Databricks-native equivalent of Snap's SETL framework. Implements the medallion architecture with the same layered approach to raw ingest, change detection, surrogate key management, and dimensional loading — but designed for Databricks DLT rather than Matillion/Snowflake.

## Architecture

```
Source Files (CSV / cloud storage)
     │
     ▼
[ Bronze ]         tbl_raw_{object}    Append-only raw ingest. Full source history retained.
     │
     ▼
[ Silver — CDC ]   tbl_cdc_{object}    Full table comparison. N/I/C/D change detection.
     │
     ▼
[ Silver — SKEY ]  tbl_skey_{object}   Surrogate key mapping. Business key → integer skey.
     │
     ▼
[ Silver — CONS ]  tbl_cons_{object}   Consolidation. Joins, skey resolution, business rules.
     │
     ▼
[ Gold ]           tbl_dim_{object}    Dimensional model. SCD-2 dimensions and fact tables.
                   tbl_mart_{object}   Aggregations and calculations for reporting.
```

| Layer | SETL Equivalent | Description |
|---|---|---|
| Bronze | STG | Raw ingest from source. Append-only — full ingestion history retained across loads. |
| Silver CDC | V_STG → PSH | Full table comparison against prior load. Writes N/C/D records; filters identicals. |
| Silver SKEY | SKEY | Surrogate key mapping. Insert-only. New skey per effective version for SCD-2 objects. |
| Silver CONS | V_CONS | Consolidation and transformation. Joins, skey resolution, business rule application. |
| Gold Dimensional | DIM / FACT | Final dimensional model. SCD-2 dimensions and fact tables. |
| Gold Data Mart | Data Mart | Aggregation views on top of the dimensional layer. |

## Project Structure

```
snap-dbx-framework/
  config_template/
    objects.yml          # Master object registry template — defines all objects and their layers
    loading.yml          # Bronze ingestion config template
    hop.yml              # Silver / gold hop config template
  config/
    objects.yml          # Populated object registry — drives config generation
    loading/             # Generated per-object bronze configs
    silver/
      cdc/               # Generated per-object CDC configs
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

Config is split across two levels:

**`config/objects.yml`** is the master registry. It defines every object in the pipeline, which layers it flows through, the table name at each layer, and whether it's metadata-driven. A generator reads this alongside `hop_defaults.yml` to produce the per-layer config files.

**Per-layer configs** (`config/loading/`, `config/silver/`, `config/gold/`) are generated from the templates in `config_template/`. Only fields that differ from the hop defaults need to be set — everything else is inherited.

See `architecture.md` for the full config schema and field reference.

## Naming Conventions

All names use `lower_snake_case`.

| Layer | Prefix | Example |
|---|---|---|
| Bronze | `tbl_raw_` | `tbl_raw_customer` |
| Silver CDC | `tbl_cdc_` | `tbl_cdc_customer` |
| Silver SKEY | `tbl_skey_` | `tbl_skey_customer` |
| Silver CONS | `tbl_cons_` | `tbl_cons_customer` |
| Gold Dimensional | `tbl_dim_` | `tbl_dim_customer` |
| Gold Data Mart | `tbl_mart_` | `tbl_mart_sales` |

## Audit Columns

| Column | Introduced |
|---|---|
| `__etl_loaded_at` | Bronze |
| `__etl_record_indicator` | Silver CDC |
| `__etl_fprint` | Silver CDC |
| `__etl_effective_from` | Silver CDC (SCD-2 only) |
| `__etl_effective_to` | Silver CDC (SCD-2 only) |
| `__etl_is_current` | Silver CDC (SCD-2 only) |
