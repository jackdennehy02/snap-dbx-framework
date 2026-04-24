# snap-dbx-framework

A YAML-driven ETL framework for Databricks, built as a Databricks-native equivalent of Snap's SETL framework. Implements the medallion architecture with the same layered approach to raw ingest, change detection, surrogate key generation, and dimensional loading — designed for Databricks DLT rather than Matillion/Snowflake.

## Architecture

```
Source Files (CSV / cloud storage)
     │
     ▼
[ Bronze — Raw ]        raw_{object}         Append-only landing. Full source history retained.
     │  Databricks Auto CDC (APPLY CHANGES INTO)
     ▼
[ Silver — CDC ]        cdc_{object}         SCD-1/2 history maintained natively by Databricks.
     │
     ▼
[ Silver — Processed ]  processed_{object}   Typecasting and cleansing. Surrogate key embedded.
     │                                        Audit columns renamed to __etl_ convention.
     ▼
[ Gold — CONS ]         cons_{object}        Consolidation. Joins, business rules, skey resolution.
     │
     ▼
[ Gold — Dimensional ]  dim_{object}         Dimensional model. SCD-2 dimensions and fact tables.
                        fact_{object}
```

| Layer | SETL Equivalent | Description |
|---|---|---|
| Bronze Raw | STG | Raw ingest from source. Append-only — full ingestion history retained. |
| Silver CDC | V_STG → PSH | Databricks Auto CDC (`APPLY CHANGES INTO`) applied to `raw_`. SCD type from object classification. |
| Silver Processed | _(none)_ + SKEY | Typecasting, cleansing, audit column renames, and surrogate key embedded directly in the table. |
| Gold CONS | V_CONS | Consolidation. Joins, skey resolution, business rule application. |
| Gold Dimensional | DIM / FACT | Final dimensional model. SCD-2 dimensions and fact tables. |

## Project Structure

```
snap-dbx-framework/
  config/
    objects.yml              # Source system registry — edit this to add objects
    config_templates/        # Template files used by the generator (not deployed)
    bronze/
      raw/                   # Generated per-object bronze raw configs
    silver/
      cdc/                   # Generated per-object silver CDC configs
      processed/             # Generated per-object silver processed configs
  resources/
    __init__.py              # DAB Python entry point — calls generate_configs() on deploy
    config_generator.py      # Generates per-layer config files from objects.yml
  src/
    bronze/
      raw.py                 # Bronze raw DLT notebook
    silver/
      cdc.py                 # Silver CDC DLT notebook
      processed.py           # Silver processed DLT notebook
  databricks.yml             # Source of truth — layer names, locations, catalog, schemas, pipeline config
  pyproject.toml             # Python project config — dependencies for DAB Python integration
```

## Adding Objects

Adding a new object is a single line in `config/objects.yml`.

### 1. Register the object

```yaml
source_systems:
  sap_erp:
    dimensions:
      material: material_key
      customer: customer_key                                    # single key
      component_bom: [parent_material_key, component_key]      # composite key
    facts:
      sales: [plant_key, material_key, customer_key]
```

- **`dimensions`** default to `scd_type: 2` (full version history) at the CDC layer.
- **`facts`** default to `scd_type: 1` (overwrite in place).
- Connection details, hop defaults, and catalog/schema values are all inherited from the source system.

### 2. Deploy

```bash
databricks bundle deploy --target dev
```

Config files are generated automatically during deployment — no separate step needed. The generator runs as part of the DAB Python integration (`resources/config_generator.py`) before files are synced to the workspace.

To generate config files locally without deploying:

```bash
python resources/config_generator.py
```

See [docs/DEPLOYMENT_INSTRUCTIONS.md](docs/DEPLOYMENT_INSTRUCTIONS.md) for the full setup and deployment steps.

## Config

**`config/objects.yml`** is the starting point for any change. Each source system defines its connection details and hop defaults once. Objects need only a name and primary key.

**`databricks.yml`** is the source of truth for layer names, medallion locations, catalog names, schema names, and pipeline environment variables. The generator reads it to resolve everything — nothing is hardcoded in config files or pipeline code. Moving a hop to a different layer means changing its `ev_lNN_location` in `databricks.yml`; the generator and notebooks adapt automatically.

Per-layer config files (in `config/bronze/`, `config/silver/`) are committed to git and deployed with the bundle. They are regenerated on every `bundle deploy` — don't edit them directly. All configuration belongs in `objects.yml`.

Gold layer configs (CONS, Dimensional) are written manually — they contain `custom_sql` that spans multiple tables and can't be generated.

See [docs/architecture.md](docs/architecture.md) for the full config schema and field reference.

## Naming Conventions

| Layer | Prefix | Example |
|---|---|---|
| Bronze Raw | `raw_` | `raw_customer` |
| Silver CDC | `cdc_` | `cdc_customer` |
| Silver Processed | `processed_` | `processed_customer` |
| Gold CONS | `cons_` | `cons_customer` |
| Gold Dimensional | `dim_` / `fact_` | `dim_customer`, `fact_sales` |

All names use `lower_snake_case`.

## Audit Columns

| Column | Introduced | Notes |
|---|---|---|
| `__etl_loaded_at` | Bronze Raw | When the record was loaded |
| `__source_updated_at` | Bronze Raw | File modification time by default; overrideable with `source_updated_at_col` in the raw config |
| `__etl_processed_at` | Silver Processed | When the record passed through processed |
| `__etl_effective_from` | Silver Processed | Renamed from Databricks `__START_AT` (SCD-2 only) |
| `__etl_effective_to` | Silver Processed | Renamed from `__END_AT`; `NULL` replaced with `ev_end_date` (SCD-2 only) |
| `__etl_is_current` | Silver Processed | Derived from `__END_AT IS NULL` (SCD-2 only) |
| `{object}_skey` | Silver Processed | MD5-based surrogate key. SCD-2: hashes business key + `__START_AT`. SCD-1: hashes business key only. |

SCD-1 objects (facts) have no versioning — effective date columns and the `__START_AT` hash component are omitted entirely.
