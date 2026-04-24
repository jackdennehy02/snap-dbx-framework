# Architecture

This framework implements the Medallion Architecture on Databricks as a Databricks-native equivalent of Snap's SETL framework. Each layer maps directly to a SETL concept, driven by YAML config wherever the logic is expressible without bespoke SQL.

---

## Layer Overview

```
Source Files (CSV / cloud storage)
     │
     ▼
[ Bronze — Raw ]        raw_{object}         ── loading.yml ──────────   Append-only landing. Full source history retained.
     │
     ▼  (Databricks Auto CDC — APPLY CHANGES INTO)
[ Silver — CDC ]        cdc_{object}         ── silver_cdc.yml ────────  SCD-1/2 history maintained by Databricks Auto CDC.
     │
     ▼
[ Silver — Processed ]  processed_{object}   ── silver_processed.yml ─   Typecasting, cleansing, audit column renames.
     │                                                                     Surrogate key embedded directly in the table.
     ▼
[ Gold — CONS ]         cons_{object}        ── hop.yml ──────────────   Consolidation. Joins, business rules, skey resolution.
     │
     ▼
[ Gold — Dimensional ]  dim_{object}         ── hop.yml ──────────────   Dimensional model.
                        fact_{object}
```

| Layer | SETL Equivalent | Config Template | Purpose |
|---|---|---|---|
| Bronze Raw | STG | `loading.yml` | Physical source connection and raw ingest. Append-only — full ingestion history retained. |
| Silver CDC | V_STG → PSH | `silver_cdc.yml` | Databricks Auto CDC (`APPLY CHANGES INTO`) applied to `raw_`. SCD type derived from object classification. |
| Silver Processed | _(none)_ + SKEY | `silver_processed.yml` | Typecasting, cleansing, audit columDT3-LBHn renames, and surrogate key generation — all in one materialized view. |
| Gold CONS | V_CONS | `hop.yml` | Consolidation and transformation. Joins processed tables, applies business rules. |
| Gold Dimensional | DIM / FACT | `hop.yml` | Final dimensional model. SCD-2 dimensions and fact tables. |

---

## Config Structure

```
config/
  objects.yml                         ← master object registry; drives file generation
  config_templates/                   ← template files used by the generator (not deployed)
    loading.yml                       ← bronze raw ingestion config template
    silver_cdc.yml                    ← silver CDC config template
    silver_processed.yml              ← silver processed config template
    hop.yml                           ← gold CONS and dimensional config template
  bronze/
    raw/                              ← generated per-object raw configs
  silver/
    cdc/                              ← generated per-object CDC configs
    processed/                        ← generated per-object processed configs
```

`objects.yml` is the orchestrator. It defines every object, what layers it flows through, and inheritable defaults. A generator reads it and produces per-layer YAML files. Blank fields in generated files inherit from the source system defaults.

**`databricks.yml` is the source of truth for architecture.** Catalog names, schema names, layer names, and medallion locations (bronze/silver) are defined there under `resources.pipelines[].configuration`. The generator reads this to resolve catalog/schema defaults and to determine which folder each hop's configs belong in. Nothing is hardcoded in pipeline code or generated config files.

---

## Bronze Raw (`loading.yml`)

Bronze raw is the raw landing zone. Records are appended on each load — the full ingestion history is retained. Schema is inferred; no column mappings are applied.

```yaml
source_type: cloud_storage | jdbc | kafka | lakehouse_federation
ingestion_mode: snapshot | streaming
merge_strategy: append                   # bronze raw is always append — full history preserved

connection:
  cloud_storage_path: /path/to/source/
  cloud_storage_type: csv | parquet | json | avro
  header: true
  delimiter: ","

source_updated_at_col:                   # optional — source column to use as __source_updated_at
                                         # defaults to file modification time if omitted
data_items: null                         # null = SELECT *
```

The framework appends two audit columns at ingest time:

| Column | Source |
|---|---|
| `__etl_loaded_at` | `current_timestamp()` |
| `__source_updated_at` | `_metadata.file_modification_time` by default. If `source_updated_at_col` is set, the framework reads that column and coerces it to timestamp — handling timestamps, dates, and strings in any common format (ISO 8601, standard datetime, US/European date formats, compact `yyyyMMdd`, etc). Falls back to a direct cast as a last resort. |

---

## Silver CDC (`silver_cdc.yml`)

Silver CDC uses Databricks' native **Auto CDC** (`APPLY CHANGES INTO`) against `raw_` to maintain an SCD history table. CDC now lives in silver — bronze is append-only landing only.

Databricks Auto CDC generates two system columns:

| Databricks Column | Description |
|---|---|
| `__START_AT` | When this version of the record became effective |
| `__END_AT` | When this version was superseded (`NULL` if current) |

These are renamed to the framework's `__etl_` convention in the Processed layer.

```yaml
object_name: cdc_{object}
source_object: raw_{object}
keys:
  - {business_key}
sequence_by: __source_updated_at
scd_type: 2                              # 1 = overwrite in place | 2 = full version history
```

`scd_type` is set automatically by the generator — `2` for dimensions, `1` for facts. Override in the per-object config if needed.

---

## Silver Processed (`silver_processed.yml`)

Processed is a materialized view that does three things in a single pass:

1. **Rename Databricks Auto CDC columns** to the `__etl_` naming convention
2. **Typecast and cleanse** — `CAST`, `TRIM`, `NULLIF`, date parsing
3. **Generate the surrogate key** — embedded directly in the table as `{object}_skey`

No joins, no business logic. If it needs a join it belongs in CONS.

**Why `materialized_view`, not `streaming_table`:** Bronze CDC is maintained by `APPLY CHANGES INTO`, which writes via MERGE. Delta streaming tables cannot read from a MERGE-modified source (`DELTA_SOURCE_TABLE_IGNORE_CHANGES`). A materialized view performs a full recompute on each refresh, bypassing this constraint.

### Framework columns (applied automatically — not in per-object YAML)

| Column | SCD-1 | SCD-2 | Source |
|---|---|---|---|
| `{object}_skey` | yes | yes | MD5 hash of business key (SCD-1) or business key + `__START_AT` (SCD-2) |
| `__source_updated_at` | yes | yes | passed through from bronze |
| `__etl_loaded_at` | yes | yes | passed through from bronze |
| `__etl_processed_at` | yes | yes | `current_timestamp()` |
| `__etl_effective_from` | — | yes | renamed from `__START_AT` |
| `__etl_effective_to` | — | yes | renamed from `__END_AT`; `NULL` → `ev_end_date` |
| `__etl_is_current` | — | yes | derived: `__END_AT IS NULL` |

SCD-1 objects have no versioning — effective date columns are omitted entirely. The surrogate key hashes business key columns only.

### Surrogate key generation

The skey is an MD5-based integer, computed as:

```
CONV(SUBSTRING(MD5(CONCAT_WS('||', col1, col2, ...)), 1, 15), 16, 10)
```

- **SCD-2**: input is `business_key_columns + __START_AT` — unique per version
- **SCD-1**: input is `business_key_columns` only — unique per natural key

Configure in the per-object processed config:

```yaml
business_key_columns:
  - {business_key}
skey_column: {object}_skey
```

### Column config (user-defined transforms)

Only columns that need transformation go in the YAML. Everything else passes through automatically.

```yaml
columns:
  - source_col: raw_amount
    target_col: amount
    data_type: DECIMAL(10,2)
    expression: "CAST(raw_amount AS DECIMAL(10,2))"

  - source_col: customer_text
    target_col: customer_text
    data_type: STRING
    expression: "NULLIF(TRIM(UPPER(customer_text)), '')"
```

---

## Gold CONS (`hop.yml`)

Consolidation is the main transformation layer. It reads from processed for business attributes and joins surrogate keys as needed. Business rules and cross-object joins live here.

`custom_sql` is the norm — consolidation logic typically spans multiple tables.

```yaml
object_name: cons_{object}
write_mode: streaming_table
read_mode: stream

custom_sql: >
  SELECT
    proc.{object}_skey,
    proc.{business_key},
    proc.{col},
    proc.__etl_effective_from,
    proc.__etl_effective_to,
    proc.__etl_is_current
  FROM STREAM(dev_setl.02_silver.processed_{object}) proc
```

Since the surrogate key is now embedded in processed, most CONS queries read directly from `processed_` without a separate key join.

---

## Gold Dimensional (`hop.yml`)

Gold defaults to `materialized_view` with `read_mode: snapshot`. Reads from CONS. Dimensions carry `__etl_effective_from/to` from silver; facts carry surrogate key references to each dimension.

```yaml
# Dimension
object_name: dim_{object}
write_mode: materialized_view
read_mode: snapshot

# Fact
object_name: fact_{object}
write_mode: materialized_view
read_mode: snapshot
```

---

## Audit Columns by Layer

| Column | Bronze Raw | Silver CDC | Silver Processed (SCD-2) | Silver Processed (SCD-1) | Gold CONS | Gold Dim |
|---|---|---|---|---|---|---|
| `{object}_skey` | — | — | yes | yes | yes | inherited |
| `__etl_loaded_at` | yes | yes | yes | yes | yes | — |
| `__source_updated_at` | yes | yes | yes | yes | — | — |
| `__etl_processed_at` | — | — | yes | yes | — | — |
| `__etl_effective_from` | — | `__START_AT` | yes | — | yes | inherited |
| `__etl_effective_to` | — | `__END_AT` | yes | — | yes | inherited |
| `__etl_is_current` | — | — | yes | — | yes | inherited |

> `__etl_effective_to` is never `NULL` from silver processed onwards — `NULL` is replaced with `ev_end_date` (`9999-12-31 23:59:59`) defined in `databricks.yml`.

---

## Naming Conventions

| Layer | Prefix | Example |
|---|---|---|
| Bronze Raw | `raw_` | `raw_customer` |
| Silver CDC | `cdc_` | `cdc_customer` |
| Silver Processed | `processed_` | `processed_customer` |
| Gold CONS | `cons_` | `cons_customer` |
| Gold Dimensional | `dim_` / `fact_` | `dim_customer`, `fact_sales` |

All names: `lower_snake_case`. No `tbl_` prefix.

---

## Framework Config (`databricks.yml`)

`databricks.yml` is the single source of truth for the pipeline architecture. Pipeline code reads all values at runtime via `spark.conf.get()` — nothing is hardcoded.

The `ev_lNN_location` variables define which medallion layer each hop belongs to. The config generator uses these to place generated config files in the correct folder (`config/bronze/`, `config/silver/`). Changing a hop's location here is all that's needed to move it between layers.

```yaml
resources:
  pipelines:
    snap_dbx_framework:
      configuration:
        ev_l01_location: bronze
        ev_l01_name:     raw
        ev_l01_catalog:  ${var.catalog}
        ev_l01_schema:   01_bronze

        ev_l02_location: silver
        ev_l02_name:     cdc
        ev_l02_catalog:  ${var.catalog}
        ev_l02_schema:   02_silver

        ev_l03_location: silver
        ev_l03_name:     processed
        ev_l03_catalog:  ${var.catalog}
        ev_l03_schema:   02_silver

        ev_end_date:        "9999-12-31 23:59:59"
        ev_start_date:      "2000-01-01 00:00:00"
        ev_null_value:      "^^"
        ev_field_separator: "||"
```

---

## When Config-Driven Isn't Enough

Use `custom_sql` when:

- **Multi-source consolidation** — joins across two or more processed objects
- **Complex business logic** — window functions, conditional aggregations, rules that don't reduce to per-column expressions
- **Non-standard skey resolution** — joining to a dimension's skey for a foreign key in a fact table

When `custom_sql` is set, the `columns` block is ignored entirely. Write it as `SELECT ... FROM STREAM({source})` for gold streaming tables, or a plain `SELECT` for materialized views.
