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
    objects.yml              # Master object registry — edit this to add objects
    config_templates/        # Template files used by the generator (not deployed)
    01_bronze/
      raw/                   # Generated per-object bronze raw configs
      cdc/                   # Generated per-object bronze CDC configs
    02_silver/
      processed/             # Generated per-object processed configs
      skey/                  # Generated per-object SKEY configs
  src/
    bronze/
      raw.py                 # Bronze raw DLT notebook
      cdc.py                 # Bronze CDC DLT notebook
    silver/
      processed.py           # Silver processed DLT notebook
      skey.py                # Silver SKEY DLT notebook
  tools/
    yaml_generator.py        # Generates per-layer config files from objects.yml
    unity_catalog_manager.py # Admin utility — manage Unity Catalog resources
    drop_all.py              # Admin utility — tear down pipeline objects
    utils.py                 # Shared helpers for pipeline notebooks
  databricks.yml             # Bundle config — catalog, schemas, pipeline definition
```

## Adding Objects

The framework is config-driven. Adding a new object to the pipeline requires two steps.

### 1. Register the object in `config/objects.yml`

Open `config/objects.yml` and add an entry for the new object. The template shows all available fields — at minimum you need `source_type`, `primary_key_columns`, and `enabled: true` on each hop you want to activate.

```yaml
objects:

  customer:
    source_type: cloud_storage
    primary_key_columns:
      - customer_key
    bronze:
      raw:
        enabled: true
        metadata_driven: true
      cdc:
        enabled: true
        metadata_driven: true
    silver:
      processed:
        enabled: true
        metadata_driven: true
      skey:
        enabled: true
        metadata_driven: true
```

Object names default to the convention (`raw_{object}`, `cdc_{object}`, etc.). Override `object_name` at any hop to deviate.

### 2. Generate the per-layer config files

```bash
pip install pyyaml   # first time only
python tools/yaml_generator.py
```

This reads `objects.yml` and writes a YAML config file for each enabled, metadata-driven hop into the appropriate subfolder under `config/`. The generator is idempotent — it never overwrites existing files, so customisations to generated configs are safe.

To generate for a specific target (e.g. prod, where catalog names differ):

```bash
python tools/yaml_generator.py --target prod
```

Commit the generated files alongside the updated `objects.yml`. Both are version-controlled — the git history is the record of exactly what was deployed and when.

### 3. Deploy

```bash
databricks bundle deploy --target dev
```

See [DEPLOYMENT_INSTRUCTIONS.md](DEPLOYMENT_INSTRUCTIONS.md) for the full setup and deployment steps.

## Config

**`config/objects.yml`** is the starting point for any change. It defines every object, which layers it flows through, and whether a layer is metadata-driven. The generator produces all per-layer configs from it.

**`databricks.yml`** is the single source of truth for catalog names, schema names, and pipeline environment variables. The generator reads it to resolve catalog/schema defaults — nothing is hardcoded in config files or pipeline code.

Per-layer config files (in `config/01_bronze/`, `config/02_silver/`) are committed to git and deployed as part of the bundle. The pipeline reads them at runtime via the `ev_config_root` parameter.

Gold layer configs (CONS, Dimensional) are written manually — they typically contain `custom_sql` that spans multiple tables and can't be generated from a simple object registry entry.

See [architecture.md](architecture.md) for the full config schema and field reference.

## Naming Conventions

All names use `lower_snake_case`.

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
