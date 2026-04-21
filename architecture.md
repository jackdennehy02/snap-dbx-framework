# Architecture

This framework implements the Medallion Architecture on Databricks as a Databricks-native equivalent of Snap's SETL framework. Each layer maps directly to a SETL concept, driven by YAML config wherever the logic is expressible without bespoke SQL.

---

## Layer Overview

```
Source Files (CSV / cloud storage)
     │
     ▼
[ Bronze — Raw ]        tbl_raw_{object}    ── loading.yml ──   Append-only raw ingest. Full source history retained.
     │
     ▼  (Databricks Auto CDC — APPLY CHANGES INTO)
[ Bronze — CDC ]        tbl_cdc_{object}    ── hop.yml ──────   SCD-2 table maintained by Databricks Auto CDC.
     │
     ▼
[ Silver — Processed ]  tbl_proc_{object}   ── hop.yml ──────   Renames Auto CDC columns to __etl_ convention.
     │                                                           Typecasting and basic cleansing.
     ▼
[ Silver — SKEY ]       tbl_skey_{object}   ── hop.yml ──────   Surrogate key mapping. Business key → integer skey.
     │
     ▼
[ Silver — CONS ]       tbl_cons_{object}   ── hop.yml ──────   Consolidation. Joins, skey resolution, business rules.
     │
     ▼
[ Gold ]                tbl_dim_{object}    ── hop.yml ──────   Dimensional model. SCD-2 dimensions and fact tables.
                        tbl_mart_{object}                       Aggregations and calculations for reporting.
```

| Layer | SETL Equivalent | Config Template | Purpose |
|---|---|---|---|
| Bronze Raw | STG | `loading.yml` | Physical source connection and raw ingest. Append-only — full ingestion history retained across loads. |
| Bronze CDC | V_STG → PSH | `hop.yml` | Databricks Auto CDC (`APPLY CHANGES INTO`) applied to `tbl_raw_`. Maintains SCD-2 history natively. |
| Silver Processed | _(no direct equivalent)_ | `hop.yml` | Renames Databricks Auto CDC columns to the `__etl_` naming convention. Typecasting and basic cleansing. |
| Silver SKEY | SKEY | `hop.yml` | Surrogate key mapping. Insert-only. New skey per effective version for SCD-2 objects. |
| Silver CONS | V_CONS | `hop.yml` | Consolidation and transformation. Joins, skey resolution, business rule application. |
| Gold Dimensional | DIM / FACT | `hop.yml` | Final dimensional model. SCD-2 dimensions and fact tables. |
| Gold Data Mart | Data Mart | `hop.yml` | Aggregation views on top of the dimensional layer. |

---

## Config Structure

Three config levels — lower levels override higher:

```
config/objects.yml                          ← master object registry; drives file generation
config_template/hop_defaults.yml            ← shared defaults per hop (write_mode, read_mode, schema)
  └── config/loading/{object}.yml           ← bronze raw ingestion config per object
  └── config/{hop}/{sublayer}/{object}.yml  ← hop-level config per object
        └── runtime parameters              ← deployment-time catalog/schema binding
```

`objects.yml` is the orchestrator. It defines every object in the pipeline, what layers it flows through, and the table name at each layer. A generator reads it alongside `hop_defaults.yml` to produce the per-layer YAML files. Blank fields inherit from the hop default.

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
| `__file_modification_time` | `_metadata.file_modification_time` — when the source file was last modified |

---

## Bronze CDC (`hop.yml`)

Bronze CDC uses Databricks' native **Auto CDC** (`APPLY CHANGES INTO`) rather than a manual view-based comparison. The framework applies `APPLY CHANGES INTO` against `tbl_raw_` to maintain a SCD-2 history table automatically.

Databricks Auto CDC generates its own system column names:

| Databricks Column | Description |
|---|---|
| `__START_AT` | When this version of the record became effective |
| `__END_AT` | When this version was superseded (`NULL` if current) |

These are renamed to the framework's `__etl_` convention in the Processed layer.

```yaml
object_name: tbl_cdc_{object}
write_mode: streaming_table
read_mode: stream
source_schema: bronze

primary_key_columns:
  - {business_key}

# tracked columns drive the SCD-2 history — changes to these create a new version
tracked_columns:
  - {col_a}
  - {col_b}
```

---

## Silver Processed (`hop.yml`)

Processed has two responsibilities:

1. **Rename Databricks Auto CDC columns** to the framework's `__etl_` naming convention
2. **Typecast and basic cleansing** — `CAST`, `TRIM`, `NULLIF`, date parsing

It reads from the bronze CDC stream. No joins, no business logic, no surrogate key resolution — if it needs a join it belongs in CONS.

The framework automatically applies the Auto CDC column renames (`__START_AT` → `__etl_effective_from`, `__END_AT` → `__etl_effective_to`, `__etl_is_current`) — these do not appear in the per-object YAML config.

```yaml
object_name: tbl_proc_{object}
write_mode: streaming_table
read_mode: stream
source_schema: bronze

columns:
  # Business columns only — typecasting and cleansing
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

## Silver SKEY (`hop.yml`)

SKEY maps each business key to an integer surrogate key. Insert-only — once a key is assigned it never changes. For SCD-2 dimensions, a new skey is generated per effective version (keyed on business key + `__etl_effective_from`).

```yaml
object_name: tbl_skey_{object}
write_mode: streaming_table
read_mode: stream
source_schema: silver       # sources from tbl_proc_{object}

primary_key_columns:
  - {business_key}
```

---

## Silver CONS (`hop.yml`)

Consolidation is the main transformation layer. It joins the processed and SKEY tables, applies business rules, and produces the clean, enriched view consumed by gold.

`custom_sql` is the norm here — consolidation logic typically spans multiple tables.

```yaml
object_name: tbl_cons_{object}
write_mode: streaming_table
read_mode: stream

custom_sql: >
  SELECT
    proc.{business_key},
    sk.{object}_skey,
    proc.{col},
    proc.__etl_effective_from,
    proc.__etl_effective_to,
    proc.__etl_record_indicator
  FROM STREAM(silver.tbl_proc_{object}) proc
  INNER JOIN silver.tbl_skey_{object} sk ON proc.{business_key} = sk.{business_key}
```

---

## Gold (`hop.yml`)

Gold defaults to `materialized_view` with `read_mode: snapshot`. Dimensions carry `__etl_effective_from/to` from silver; facts carry surrogate key references to each dimension.

```yaml
# Dimension
object_name: tbl_dim_{object}
write_mode: materialized_view
read_mode: snapshot
source_schema: silver

# Fact / Data Mart
object_name: tbl_mart_{object}
write_mode: materialized_view
read_mode: snapshot
```

---

## Audit Columns by Layer

| Column | Bronze Raw | Bronze CDC | Silver Processed | Silver SKEY | Silver CONS | Gold |
|---|---|---|---|---|---|---|
| `__etl_loaded_at` | yes | — | yes | yes | yes | — |
| `__file_modification_time` | yes | — | — | — | — | — |
| `__etl_effective_from` | — | _(Auto CDC — `__START_AT`)_ | yes (renamed) | SCD-2 | SCD-2 | inherited |
| `__etl_effective_to` | — | _(Auto CDC — `__END_AT`)_ | yes (mapped) | — | SCD-2 | inherited |
| `__etl_is_current` | — | — | yes (derived) | — | SCD-2 | inherited |

> `__etl_record_indicator` is not used — Auto CDC handles change detection internally.
>
> `__etl_effective_to` is never `NULL`. In Processed, `__END_AT IS NULL` (current record) is replaced with `ev_end_date` (`9999-12-31 23:59:59`). `__etl_is_current` is then derived as `__etl_effective_to = ev_end_date`.

---

## Naming Conventions

| Layer | Table Prefix | Example |
|---|---|---|
| Bronze Raw | `tbl_raw_` | `tbl_raw_customer` |
| Bronze CDC | `tbl_cdc_` | `tbl_cdc_customer` |
| Silver Processed | `tbl_proc_` | `tbl_proc_customer` |
| Silver SKEY | `tbl_skey_` | `tbl_skey_customer` |
| Silver CONS | `tbl_cons_` | `tbl_cons_customer` |
| Gold Dimensional | `tbl_dim_` | `tbl_dim_customer` |
| Gold Data Mart | `tbl_mart_` | `tbl_mart_sales` |

All names: `lower_snake_case`.

---

## Environment Variables

Pipeline-wide constants controlled outside of YAML config. These mirror the environment variables used in SETL and are bound at runtime.

| Variable | Value | Purpose |
|---|---|---|
| `ev_end_date` | `9999-12-31 23:59:59` | Default effective-to for current records. Used in Processed to replace `__END_AT IS NULL`. |
| `ev_start_date` | `2000-01-01 00:00:00` | Default effective-from for initial loads. |
| `ev_null_value` | `^^` | Consistent null sentinel applied across the pipeline. |
| `ev_field_separator` | `\|\|` | Field separator for compounding business keys. |

---

## When Config-Driven Isn't Enough

Use `custom_sql` when:

- **Skey resolution** — joining processed to SKEY tables (standard pattern for all CONS objects)
- **Multi-source consolidation** — joins across two or more processed objects
- **Complex business logic** — window functions, conditional aggregations, or rules that don't reduce to per-column expressions

When `custom_sql` is set, the `columns` block is ignored entirely. Write it as a `SELECT` from `STREAM({source})` for silver streaming tables, or a plain `SELECT` from silver tables for gold snapshot reads.
