# CLAUDE.md — snap-dbx-framework

Before doing any work in this project, read the following documents in order.

---

## Required Reading

### 1. Architecture
**[architecture.md](architecture.md)**
Canonical reference for pipeline layers, SETL mapping, naming conventions, audit columns, surrogate key generation, and the config inheritance model.

### 2. Master Object Registry
**[config/objects.yml](../config/objects.yml)**
Defines every object in the pipeline. Hop defaults are keyed by layer ID (`l01`, `l02`, `l03`) — not by bronze/silver names. The generator reads this to produce per-layer config files.

### 3. Config Templates
**[config/config_templates/loading.yml](../config/config_templates/loading.yml)** — bronze raw ingestion config schema.
**[config/config_templates/silver_cdc.yml](../config/config_templates/silver_cdc.yml)** — silver CDC config schema.
**[config/config_templates/silver_processed.yml](../config/config_templates/silver_processed.yml)** — silver processed config schema (includes surrogate key fields).
**[config/config_templates/hop.yml](../config/config_templates/hop.yml)** — gold CONS and dimensional config schema.

### 4. Project Context
**[../../raw_docs/SETL_framework_matillion](../../raw_docs/SETL_framework_matillion)** — Snap's SETL framework documentation. This is the Matillion/Snowflake implementation this project mirrors on Databricks.
**[../../raw_docs/final_target_potential_data_model.txt](../../raw_docs/final_target_potential_data_model.txt)** — Target data model (DBML format). Gold layer table definitions, relationships, and SCD-2 decisions.
**[../../project_brief](../../project_brief)** — Project brief. Scope, goals, and constraints.

---

## Key Decisions to Carry Forward

### Architecture

- **Three hops: raw → cdc → processed.** Bronze is append-only landing only. CDC and processed both live in silver. Gold (CONS, dimensional) is manually configured.
- **CDC lives in silver, not bronze.** `src/silver/cdc.py` applies `APPLY CHANGES INTO` from `raw_` to `cdc_`. Bronze raw is intentionally dumb — it just lands data.
- **The SKEY layer is eliminated.** Surrogate keys are generated directly inside the processed materialized view, not in a separate mapping table.
- **`databricks.yml` is the source of truth for architecture.** Layer names (`ev_lNN_name`), medallion locations (`ev_lNN_location`), catalog, and schema are all defined there. The config generator reads these to determine where to write config files and what catalog/schema to target. Nothing is hardcoded in notebooks or config files.

### Config generation

- **Config folders are derived from `databricks.yml`.** The generator places files at `config/{ev_lNN_location}/{ev_lNN_name}/`. Currently: `config/bronze/raw/`, `config/silver/cdc/`, `config/silver/processed/`.
- **`objects.yml` uses `l01`/`l02`/`l03` for hop defaults** — not bronze/silver. The bronze/silver distinction belongs to `databricks.yml`.
- **Generated configs are committed to git.** They are regenerated on every `bundle deploy` — don't edit them directly.

### Silver processed

- **Materialized view, not streaming table.** `APPLY CHANGES INTO` writes via MERGE; streaming tables cannot read from MERGE-modified sources (`DELTA_SOURCE_TABLE_IGNORE_CHANGES`).
- **Surrogate key is MD5-based integer.** SCD-2: hash of `business_key_columns + __START_AT`. SCD-1: hash of `business_key_columns` only. Configured via `business_key_columns` and `skey_column` in the processed config.
- **Skey appears at ordinal position 0** in every processed table.
- **Framework transforms are automatic** — `__START_AT` → `__etl_effective_from`, `__END_AT` → `__etl_effective_to` (NULL → `ev_end_date`), `__etl_is_current` derived from `__END_AT IS NULL`. Never add these to the `columns` config.
- **Passthrough is automatic.** Only columns needing transformation go in `columns`. Everything else passes through unchanged.

### Bronze raw

- **`__source_updated_at`** defaults to `_metadata.file_modification_time`. Override with `source_updated_at_col` in the raw config if the source data contains a timestamp column.
- **`__etl_loaded_at`** is always `current_timestamp()` at load time.
- Both audit columns pass through CDC and are available in silver processed.

### Naming

- Prefixes: `raw_`, `cdc_`, `processed_`, `cons_`, `dim_`, `fact_`. No `skey_`, no `tbl_`.
- All `lower_snake_case`.
