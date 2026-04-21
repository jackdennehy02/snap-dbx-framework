# Architecture

This framework implements the Medallion Architecture on Databricks as a Databricks-native equivalent of Snap's SETL framework. Each layer maps directly to a SETL concept, driven by YAML config wherever the logic is expressible without bespoke SQL.

---

## Layer Overview

```
Source Files (CSV / cloud storage)
     │
     ▼
[ Bronze — Raw ]        raw_{object}         ── loading.yml ──────   Append-only raw ingest. Full source history retained.
     │
     ▼  (Databricks Auto CDC — APPLY CHANGES INTO)
[ Bronze — CDC ]        cdc_{object}         ── bronze_cdc.yml ───   SCD-2 table maintained by Databricks Auto CDC.
     │
     ▼
[ Silver — Processed ]  processed_{object}   ── silver_processed.yml   Renames Auto CDC columns to __etl_ convention.
     │                                                                  Typecasting and basic cleansing.
     ▼
[ Silver — SKEY ]       skey_{object}        ── silver_skey.yml ──   Surrogate key mapping. Business key → integer skey.
     │
     ▼
[ Gold — CONS ]         cons_{object}        ── hop.yml ──────────   Consolidation. Joins processed + SKEY, business rules.
     │
     ▼
[ Gold — Dimensional ]  dim_{object}         ── hop.yml ──────────   Dimensional model. SCD-2 dimensions and fact tables.
                        fact_{object}
```

| Layer | SETL Equivalent | Config Template | Purpose |
|---|---|---|---|
| Bronze Raw | STG | `loading.yml` | Physical source connection and raw ingest. Append-only — full ingestion history retained across loads. |
| Bronze CDC | V_STG → PSH | `bronze_cdc.yml` | Databricks Auto CDC (`APPLY CHANGES INTO`) applied to `raw_`. Maintains SCD-2 history natively. |
| Silver Processed | _(no direct equivalent)_ | `silver_processed.yml` | Renames Databricks Auto CDC columns to the `__etl_` naming convention. Typecasting and basic cleansing. |
| Silver SKEY | SKEY | `silver_skey.yml` | Surrogate key mapping. Insert-only. New skey per effective version for SCD-2 objects. |
| Gold CONS | V_CONS | `hop.yml` | Consolidation and transformation. Joins processed + SKEY, skey resolution, business rule application. |
| Gold Dimensional | DIM / FACT | `hop.yml` | Final dimensional model. SCD-2 dimensions and fact tables. |

---

## Config Structure

```
config/framework.yml                        ← centralised catalogs, schemas, and environment variables
config/objects.yml                          ← master object registry; drives file generation
config_templates/                           ← template files used by the generator
  └── config/01_bronze/raw/{object}.yml     ← bronze raw ingestion config per object
  └── config/01_bronze/cdc/{object}.yml     ← bronze CDC config per object
  └── config/02_silver/processed/{object}.yml
  └── config/02_silver/skey/{object}.yml
  └── config/03_gold/consolidation/{object}.yml
  └── config/03_gold/dimensional/{object}.yml
```

`objects.yml` is the orchestrator. It defines every object in the pipeline, what layers it flows through, and the table name at each layer. A generator reads it to produce the per-layer YAML files. Blank fields inherit from the hop default.

`framework.yml` is the single source of truth for catalog names, schema names, and pipeline environment variables. All pipelines load from it — nothing is hardcoded in pipeline code.

---

## Bronze Raw (`loading.yml`)

Bronze raw is the raw landing zone. Records are appended on each load — the full ingestion history is retained across loads. Schema is inferred; no column mappings are applied.

```yaml
source_type: cloud_storage | jdbc | kafka | lakehouse_federation
ingestion_mode: snapshot | streaming
merge_strategy: append                   # bronze raw is always append — full history preserved
object_category: reference | transactional | dimension | fact

connection:
  source_connection: <volume path>
  cloud_storage_type: csv | parquet | json | avro
  header: true
  delimiter: ","

data_items: null                         # null = SELECT *
```

The framework appends two audit columns at ingest time:

| Column | Source |
|---|---|
| `__etl_loaded_at` | `current_timestamp()` — when the record was loaded |
| `__source_updated_at` | `_metadata.file_modification_time` — when the source file was last modified; standardised name across all source types |

---

## Bronze CDC (`bronze_cdc.yml`)

Bronze CDC uses Databricks' native **Auto CDC** (`APPLY CHANGES INTO`) rather than a manual view-based comparison. The framework applies `APPLY CHANGES INTO` against `raw_` to maintain a SCD-2 history table automatically.

Databricks Auto CDC generates its own system column names:

| Databricks Column | Description |
|---|---|
| `__START_AT` | When this version of the record became effective |
| `__END_AT` | When this version was superseded (`NULL` if current) |

These are renamed to the framework's `__etl_` convention in the Processed layer.

```yaml
object_name: cdc_{object}
source_object: raw_{object}
write_mode: streaming_table
read_mode: stream

keys:
  - {business_key}

sequence_by: __source_updated_at
scd_type: 2
```

---

## Silver Processed (`silver_processed.yml`)

Processed has two responsibilities:

1. **Rename Databricks Auto CDC columns** to the framework's `__etl_` naming convention
2. **Typecast and basic cleansing** — `CAST`, `TRIM`, `NULLIF`, date parsing

It reads from the bronze CDC stream. No joins, no business logic, no surrogate key resolution — if it needs a join it belongs in CONS.

The framework automatically applies these columns — they do not appear in the per-object YAML config:

| Framework Column | SCD-1 | SCD-2 | Source |
|---|---|---|---|
| `__etl_loaded_at` | yes | yes | passed through from bronze |
| `__etl_processed_at` | yes | yes | `current_timestamp()` |
| `__etl_effective_from` | — | yes | renamed from `__START_AT` |
| `__etl_effective_to` | — | yes | renamed from `__END_AT`; `NULL` → `ev_end_date` |
| `__etl_is_current` | — | yes | derived: `__END_AT IS NULL` |

SCD-1 objects have no versioning — effective date columns are omitted entirely rather than populated with meaningless constants.

Only business columns (typecasting, cleansing) go in the YAML config. Everything else passes through automatically.

```yaml
object_name: processed_{object}
source_object: cdc_{object}
write_mode: streaming_table
read_mode: stream

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

## Silver SKEY (`silver_skey.yml`)

SKEY is a pure key mapping table. It maps each business key to an integer surrogate key generated via MD5 hash. Insert-only — once a key is assigned it never changes.

**SCD-1** (`scd_type: 1`): hash of business key only — one skey per unique natural key.
**SCD-2** (`scd_type: 2`): hash of business key + `__etl_effective_from` — one skey per version.

Output columns:

| Column | SCD-1 | SCD-2 |
|---|---|---|
| `{object}_skey` | yes | yes |
| business key column(s) | yes | yes |
| `__etl_effective_from` | — | yes |
| `__etl_effective_to` | — | yes (for range joins in CONS) |

CONS reads from processed for business attributes and joins SKEY at the end to resolve surrogate keys. SKEY carries no business columns — only the mapping.

```yaml
object_name: skey_{object}
source_object: processed_{object}
write_mode: streaming_table
read_mode: stream

scd_type: 2
business_key_columns:
  - {business_key}
skey_column: {object}_skey
```

---

## Gold CONS (`hop.yml`)

Consolidation is the main transformation layer. It reads from processed for business attributes and joins SKEY tables to resolve surrogate keys. Business rules and cross-object joins live here.

`custom_sql` is the norm — consolidation logic typically spans multiple tables.

```yaml
object_name: cons_{object}
write_mode: streaming_table
read_mode: stream

custom_sql: >
  SELECT
    sk.{object}_skey,
    proc.{business_key},
    proc.{col},
    proc.__etl_effective_from,
    proc.__etl_effective_to,
    proc.__etl_is_current
  FROM STREAM(snap_dbx.02_silver.processed_{object}) proc
  INNER JOIN snap_dbx.02_silver.skey_{object} sk
    ON proc.{business_key} = sk.{business_key}
    AND proc.__etl_effective_from = sk.__etl_effective_from
```

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

| Column | Bronze Raw | Bronze CDC | Silver Processed (SCD-2) | Silver Processed (SCD-1) | Silver SKEY (SCD-2) | Gold CONS | Gold Dim |
|---|---|---|---|---|---|---|---|
| `__etl_loaded_at` | yes | yes | yes | yes | — | yes | — |
| `__source_updated_at` | yes | yes | yes | yes | — | — | — |
| `__etl_processed_at` | — | — | yes | yes | — | — | — |
| `__etl_effective_from` | — | `__START_AT` | yes | — | yes | yes | inherited |
| `__etl_effective_to` | — | `__END_AT` | yes | — | yes | yes | inherited |
| `__etl_is_current` | — | — | yes | — | — | yes | inherited |

> `__etl_effective_to` is never `NULL` from silver onwards — `NULL` is replaced with `ev_end_date` (`9999-12-31 23:59:59`) from `framework.yml`.

---

## Naming Conventions

| Layer | Prefix | Example |
|---|---|---|
| Bronze Raw | `raw_` | `raw_customer` |
| Bronze CDC | `cdc_` | `cdc_customer` |
| Silver Processed | `processed_` | `processed_customer` |
| Silver SKEY | `skey_` | `skey_customer` |
| Gold CONS | `cons_` | `cons_customer` |
| Gold Dimensional | `dim_` / `fact_` | `dim_customer`, `fact_sales` |

All names: `lower_snake_case`. No `tbl_` prefix. No layer-letter sub-prefixes.

---

## Framework Config (`framework.yml`)

Pipeline-wide constants defined in `config/framework.yml`. All pipelines load from this file at runtime — values are never hardcoded in pipeline code.

```yaml
catalogs:
  bronze: snap_dbx
  silver: snap_dbx
  gold: snap_dbx

schemas:
  bronze: 01_bronze
  silver: 02_silver
  gold: 03_gold

environment:
  ev_end_date: "9999-12-31 23:59:59"
  ev_start_date: "2000-01-01 00:00:00"
  ev_null_value: "^^"
  ev_field_separator: "||"
```

---

## When Config-Driven Isn't Enough

Use `custom_sql` when:

- **Skey resolution** — joining processed to SKEY tables (standard pattern for all CONS objects)
- **Multi-source consolidation** — joins across two or more processed objects
- **Complex business logic** — window functions, conditional aggregations, or rules that don't reduce to per-column expressions

When `custom_sql` is set, the `columns` block is ignored entirely. Write it as a `SELECT` from `STREAM({source})` for gold streaming tables, or a plain `SELECT` for materialized views.
