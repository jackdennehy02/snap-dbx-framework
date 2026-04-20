# Architecture

This framework implements the Medallion Architecture on Databricks as a Databricks-native equivalent of Snap's SETL framework. Each layer maps directly to a SETL concept, driven by YAML config wherever the logic is expressible without bespoke SQL.

---

## Layer Overview

```
Source Files (CSV / cloud storage)
     │
     ▼
[ Bronze — Raw ]      tbl_raw_{object}    ── loading.yml ──   Append-only raw ingest. Full source history retained.
     │
     │  vw_raw_{object}   — view of current raw load
     │  vw_cdc_{object}   — compares vw_raw to tbl_cdc; derives N/I/C/D indicator
     ▼
[ Bronze — CDC ]      tbl_cdc_{object}    ── hop.yml ──────   N/C/D records inserted. Identical records filtered.
     │
     ▼
[ Silver — Processed ]  tbl_proc_{object}  ── hop.yml ──────  Typecasting and basic cleansing. Reads CDC stream.
     │
     ▼
[ Silver — SKEY ]     tbl_skey_{object}   ── hop.yml ──────   Surrogate key mapping. Business key → integer skey.
     │
     ▼
[ Silver — CONS ]     tbl_cons_{object}   ── hop.yml ──────   Consolidation. Joins, skey resolution, business rules.
     │
     ▼
[ Gold ]              tbl_dim_{object}    ── hop.yml ──────   Dimensional model. SCD-2 dimensions and fact tables.
                      tbl_mart_{object}                       Aggregations and calculations for reporting.
```

| Layer | SETL Equivalent | Config Template | Purpose |
|---|---|---|---|
| Bronze Raw | STG | `loading.yml` | Physical source connection and raw ingest. Append-only — full ingestion history retained across loads. |
| Bronze CDC | V_STG → PSH | `hop.yml` | Full table comparison via views. Inserts N/C/D records; filters identicals. |
| Silver Processed | _(no direct equivalent)_ | `hop.yml` | Typecasting and basic cleansing. Reads from bronze CDC stream. |
| Silver SKEY | SKEY | `hop.yml` | Surrogate key mapping. Insert-only. New skey per effective version for SCD-2 objects. |
| Silver CONS | V_CONS | `hop.yml` | Consolidation and transformation. Joins, skey resolution, business rule application. |
| Gold Dimensional | DIM / FACT | `hop.yml` | Final dimensional model. SCD-2 dimensions and fact tables. |
| Gold Data Mart | Data Mart | `hop.yml` | Aggregation views on top of the dimensional layer. |

---

## Config Structure

Three config levels — lower levels override higher:

```
config/objects.yml                        ← master object registry; drives file generation
config_template/hop_defaults.yml          ← shared defaults per hop (write_mode, read_mode, schema)
  └── config/loading/{object}.yml         ← bronze raw ingestion config per object
  └── config/{hop}/{sublayer}/{object}.yml ← hop-level config per object
        └── runtime parameters            ← deployment-time catalog/schema binding
```

`objects.yml` is the orchestrator. It defines every object in the pipeline, what layers it flows through, and the table name at each layer. A generator reads it alongside `hop_defaults.yml` to produce the per-layer YAML files. Blank fields inherit from the hop default.

---

## Bronze Raw (`loading.yml`)

Bronze raw is the raw landing zone. Records are appended on each load — the full ingestion history is retained, including duplicates and source errors. Schema is inferred; no column mappings are applied.

```yaml
source_type: cloud_storage | jdbc | kafka | lakehouse_federation
ingestion_mode: snapshot | streaming
merge_strategy: append                   # bronze raw is always append — full history preserved
object_category: reference | transactional | dimension | fact

connection:
  # cloud_storage
  source_connection: <volume path>
  cloud_storage_type: csv | parquet | json | avro
  header: true
  delimiter: ","

data_items: null                         # null = SELECT *
```

`__etl_loaded_at` is appended by the framework at ingest time.

---

## Bronze CDC (`hop.yml`)

CDC lives in bronze. Two views are created each run before the CDC table is written:

- **`vw_raw_{object}`** — a view over the current raw load (latest records from `tbl_raw_`)
- **`vw_cdc_{object}`** — compares `vw_raw_` against the existing `tbl_cdc_` table to derive the change indicator

The CDC table is then written by inserting only N, C, and D records. Identical records are filtered and never written.

Change indicators follow SETL conventions:

| Indicator | Meaning |
|---|---|
| N | New — business key not seen before |
| I | Identical — fingerprint matches prior load (filtered out, not written) |
| C | Changed — business key seen before, fingerprint differs |
| D | Deleted — business key present in prior CDC but absent from current full load |

```yaml
object_name: tbl_cdc_{object}
write_mode: streaming_table
read_mode: stream
source_schema: bronze

primary_key_columns:
  - {business_key}

columns:
  - source_col: {col}
    target_col: {col}
    data_type: STRING
    is_tracked: true        # contributes to __etl_fprint; change in tracked field = C indicator
```

`__etl_record_indicator` and `__etl_fprint` are added at this layer.

---

## Silver Processed (`hop.yml`)

The processed layer reads directly from the bronze CDC stream. Its sole purpose is typecasting and basic cleansing — no joins, no business logic, no surrogate key resolution. It produces clean, correctly-typed records for the SKEY layer.

Typical operations:
- `CAST(raw_col AS DECIMAL(10,2))`
- `NULLIF(TRIM(UPPER(text_col)), '')`
- Parsing composite fields (e.g. splitting a text date into a proper `DATE`)

```yaml
object_name: tbl_proc_{object}
write_mode: streaming_table
read_mode: stream
source_schema: bronze       # sources from tbl_cdc_{object}

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

No SCD-2 logic, no fingerprinting, no joins. If it requires a join, it belongs in CONS.

---

## Silver SKEY (`hop.yml`)

SKEY maps each business key to an integer surrogate key. Insert-only — once a key is assigned it never changes. For SCD-2 dimensions, a new skey is generated per effective version (keyed on business key + effective from date).

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

Consolidation is the main transformation layer. It joins the processed and SKEY tables, applies business rules, resolves surrogate keys, and produces the clean, enriched view consumed by gold.

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
    ...
  FROM STREAM(silver.tbl_proc_{object}) proc
  INNER JOIN silver.tbl_skey_{object} sk ON proc.{business_key} = sk.{business_key}
```

---

## Gold (`hop.yml`)

Gold defaults to `materialized_view` with `read_mode: snapshot`. Dimensions carry SCD-2 audit columns from silver; facts carry surrogate key references to each dimension.

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
| `__etl_loaded_at` | yes | yes | yes | yes | yes | — |
| `__etl_record_indicator` | — | yes | yes | — | yes | — |
| `__etl_fprint` | — | yes | — | — | — | — |
| `__etl_effective_from` | — | SCD-2 | SCD-2 | SCD-2 | SCD-2 | inherited |
| `__etl_effective_to` | — | SCD-2 | — | — | SCD-2 | inherited |
| `__etl_is_current` | — | SCD-2 | — | — | SCD-2 | inherited |

---

## Naming Conventions

| Layer | Table Prefix | View Prefix | Example |
|---|---|---|---|
| Bronze Raw | `tbl_raw_` | `vw_raw_` | `tbl_raw_customer`, `vw_raw_customer` |
| Bronze CDC | `tbl_cdc_` | `vw_cdc_` | `tbl_cdc_customer`, `vw_cdc_customer` |
| Silver Processed | `tbl_proc_` | — | `tbl_proc_customer` |
| Silver SKEY | `tbl_skey_` | — | `tbl_skey_customer` |
| Silver CONS | `tbl_cons_` | — | `tbl_cons_customer` |
| Gold Dimensional | `tbl_dim_` | — | `tbl_dim_customer` |
| Gold Data Mart | `tbl_mart_` | — | `tbl_mart_sales` |

All names: `lower_snake_case`.

---

## When Config-Driven Isn't Enough

Use `custom_sql` when:

- **Skey resolution** — joining processed to SKEY tables (standard pattern for all CONS objects)
- **Multi-source consolidation** — joins across two or more processed objects
- **Complex business logic** — window functions, conditional aggregations, or rules that don't reduce to per-column expressions

When `custom_sql` is set, the `columns` block is ignored entirely. Write it as a `SELECT` from `STREAM({source})` for silver streaming tables, or a plain `SELECT` from silver tables for gold snapshot reads.
