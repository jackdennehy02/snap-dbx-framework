# CLAUDE.md — snap-dbx-framework

Before doing any work in this project, read the following documents in order. They contain the architecture decisions, data model, SETL framework context, and config schemas that inform everything here.

---

## Required Reading

### 1. Architecture
**[architecture.md](architecture.md)**
The canonical reference for the pipeline layers, SETL mapping, naming conventions, audit columns, and config inheritance model. If something contradicts this doc, flag it rather than assuming.

### 2. Master Object Registry
**[config/objects.yml](config/objects.yml)**
Defines every object in the pipeline — what layers it flows through, its table name at each layer, load behaviour, and whether it's metadata-driven. The generator uses this to produce per-layer config files.

### 3. Config Templates
**[config_template/objects.yml](config_template/objects.yml)** — schema and field documentation for the master object registry.
**[config_template/loading.yml](config_template/loading.yml)** — schema for bronze raw ingestion configs (`config/loading/`).
**[config_template/hop.yml](config_template/hop.yml)** — schema for all hop configs (bronze CDC, silver, gold).

### 4. Project Context
**[../../raw_docs/SETL_framework_matillion](../../raw_docs/SETL_framework_matillion)** — Snap's SETL framework documentation. This is the Matillion/Snowflake implementation this project mirrors on Databricks.
**[../../raw_docs/final_target_potential_data_model.txt](../../raw_docs/final_target_potential_data_model.txt)** — Target data model (DBML format). Gold layer table definitions, relationships, and SCD-2 decisions.
**[../../project_brief](../../project_brief)** — Project brief. Scope, goals, and constraints.

---

## Key Decisions to Carry Forward

- **Bronze has two sub-layers: raw and CDC.**
  - Raw (`tbl_raw_`) is append-only — every load is retained as full source history.
  - CDC (`tbl_cdc_`) uses **Databricks Auto CDC** (`APPLY CHANGES INTO`) applied to `tbl_raw_`. Databricks maintains the SCD-2 history natively — no manual view comparison. Databricks generates its own system column names (`__START_AT`, `__END_AT`, `_change_type`).
- **Silver processed (`tbl_proc_`) has two jobs:** apply framework audit column transforms, then user-defined typecasting and basic cleansing. No joins, no business logic — if it needs a join it belongs in CONS. Framework transforms (always applied, never in YAML config): `__etl_loaded_at` (pass-through), `__etl_processed_at` (`current_timestamp()`), `__START_AT` → `__etl_effective_from`, `__END_AT` → `__etl_effective_to` (`NULL` → `9999-12-31 23:59:59`), `__etl_is_current` (`__END_AT IS NULL`). Unspecified columns pass through automatically.
- **Silver processed is a materialized view reading from the bronze CDC table** (snapshot, not stream). `APPLY CHANGES INTO` writes via MERGE — streaming tables cannot read from a MERGE-modified source (`DELTA_SOURCE_TABLE_IGNORE_CHANGES`). Silver SKEY is also a materialized view for the same reason (its source, processed, is a MV).
- **SKEY is insert-only.** Once a surrogate key is assigned it never changes. SCD-2 objects generate a new skey per effective version.
- **Naming convention:** `tbl_raw_`, `tbl_cdc_`, `tbl_proc_`, `tbl_skey_`, `tbl_cons_`, `tbl_dim_`, `tbl_mart_`. All `lower_snake_case`. No views needed — Auto CDC handles the comparison.
- **`__etl_record_indicator` is dropped** — Auto CDC handles change detection internally. Record state is determined by `__etl_is_current` (derived from `__END_AT IS NULL`).
- **`__etl_` columns are introduced at Silver Processed**, not bronze. Bronze CDC uses Databricks system column names (`__START_AT`, `__END_AT`).
- **`__file_modification_time` is renamed to `__source_updated_at` at Bronze Raw** — standardised name across all source types. Both `__source_updated_at` and `__etl_loaded_at` pass through CDC and are available in Silver Processed.
- **objects.yml is the orchestrator.** It drives generation of all per-layer config files. Don't edit individual layer configs without checking objects.yml first.
- **Gold is the dimensional model.** Dimensions are SCD-2 for customer and material; plant is SCD-1. Facts carry surrogate keys resolved in the CONS layer.
