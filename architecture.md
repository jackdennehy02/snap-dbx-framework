# Architecture

This framework implements the Medallion Architecture on Databricks as a Databricks-native equivalent of Snap's SETL framework. Each layer maps directly to a SETL concept, driven by YAML config wherever the logic is expressible without bespoke SQL.

---

## Layer Overview

```
Source Files (CSV / cloud storage)
     │
     ▼
[ Bronze ]         tbl_raw_{object}     ── loading.yaml ──   Append-only raw ingest. Full source history retained.
     │
     ▼
[ Silver — CDC ]   tbl_cdc_{object}     ── hop.yaml ──────   Full table comparison. N/I/C/D change detection.
     │
     ▼
[ Silver — SKEY ]  tbl_skey_{object}    ── hop.yaml ──────   Surrogate key mapping. Business key → integer skey.
     │
     ▼
[ Silver — CONS ]  tbl_cons_{object}    ── hop.yaml ──────   Consolidation. Joins, skey resolution, business rules.
     │
     ▼
[ Gold ]           tbl_dim_{object}     ── hop.yaml ──────   Dimensional model. SCD-2 dimensions and fact tables.
                   tbl_mart_{object}
```

| Layer | SETL Equivalent | Config Template | Purpose |
|---|---|---|---|
| Bronze | STG | `loading.yaml` | Physical source connection and raw ingest. Append-only — records accumulate across loads to form a permanent raw history. |
| Silver (CDC) | V_STG → PSH | `hop.yaml` | Full table comparison against prior load. Assigns record indicator (N/I/C/D) and writes non-identical records to history. |
| Silver (SKEY) | SKEY | `hop.yaml` | Surrogate key mapping. Derives an integer skey per business key, insert-only. |
| Silver (CONS) | V_CONS | `hop.yaml` | Consolidation and transformation. Joins, skey resolution, business rule application. |
| Gold (Dimensional) | DIM / FACT | `hop.yaml` | Final dimensional model. SCD-2 dimensions and fact tables. |
| Gold (Data Mart) | Data Mart | `hop.yaml` | Aggregation and calculation views on top of the dimensional layer. |

---

## Config Structure

Three config levels — lower levels override higher:

```
config/objects.yaml                     ← master object registry; drives file generation
config_template/hop_defaults.yaml       ← shared defaults per hop (write_mode, read_mode, schema)
  └── config/loading/{object}.yml       ← bronze ingestion config per object
  └── config/{hop}/{object}.yml         ← hop-level config per object (silver/gold)
        └── runtime parameters          ← deployment-time catalog/schema binding
```

`objects.yaml` is the orchestrator. It defines every object in the pipeline, what layers it flows through, and the table name at each layer. A generator reads it alongside `hop_defaults.yaml` to produce the per-layer YAML files. Blank fields in those files inherit from the hop default.

---

## Bronze (`loading.yaml`)

Bronze is the raw landing zone. Records are appended on each load — bronze retains the full ingestion history, including duplicates and source errors. Schema is inferred; no column mappings are applied.

```yaml
source_type: cloud_storage | jdbc | kafka | lakehouse_federation
ingestion_mode: snapshot | streaming
merge_strategy: append                   # bronze is always append — full history is preserved
object_category: reference | transactional | dimension | fact

connection:
  # cloud_storage
  source_connection: <volume path>
  cloud_storage_type: csv | parquet | json | avro
  header: true
  delimiter: ","

  # jdbc / kafka / lakehouse_federation — see loading.yaml template

data_items: null                         # null = SELECT *
```

`__etl_loaded_at` is appended by the framework at ingest time. `__etl_record_indicator` and `__etl_fprint` are added in the Silver CDC layer.

---

## Silver — CDC (`hop.yaml`)

CDC performs a full table comparison between the current bronze load and what has already been written to the CDC table. It assigns each record a change indicator and writes only non-identical records through, preserving every change as a new row.

Change indicators follow SETL conventions:

| Indicator | Meaning |
|---|---|
| N | New — business key not seen before |
| I | Identical — fingerprint matches prior load (filtered out, not written) |
| C | Changed — business key seen before, fingerprint differs |
| D | Deleted — business key present in prior load but absent from current full load |

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
    is_tracked: true        # contributes to __etl_fprint; change in any tracked field = C indicator
```

**SCD-2 vs SCD-1 at CDC:**
- SCD-2 (history tracked): set `is_tracked: true` on fields that should drive a new row on change. Include the full set of `__etl_effective_*` audit columns.
- SCD-1 (overwrite in place): set `is_tracked: false`. The fingerprint still detects changes, but no new row is added — the existing record is updated.

---

## Silver — SKEY (`hop.yaml`)

SKEY maps each business key to an integer surrogate key. Insert-only — once a key is assigned it never changes. For SCD-2 dimensions, a new skey is generated per effective version (keyed on business key + effective from date).

```yaml
object_name: tbl_skey_{object}
write_mode: streaming_table
read_mode: stream
source_schema: silver       # sources from tbl_cdc_{object}

primary_key_columns:
  - {business_key}

columns:
  - source_col: {business_key}
    target_col: {business_key}
    data_type: STRING
  - source_col:
    target_col: {object}_skey
    data_type: BIGINT
    expression: "MD5_NUMBER_LOWER64({business_key})"   # or hash of key + effective_from for SCD-2
```

---

## Silver — CONS (`hop.yaml`)

Consolidation is the main transformation layer. It joins the CDC and SKEY tables, applies business rules, resolves surrogate keys, and produces the clean, enriched view consumed by gold.

`custom_sql` is the norm here — consolidation logic typically spans multiple tables and can't be expressed as single-object column mappings.

```yaml
object_name: tbl_cons_{object}
write_mode: streaming_table
read_mode: stream

custom_sql: >
  SELECT
    cdc.{business_key},
    sk.{object}_skey,
    cdc.{col},
    ...
  FROM STREAM(silver.tbl_cdc_{object}) cdc
  INNER JOIN silver.tbl_skey_{object} sk ON cdc.{business_key} = sk.{business_key}
```

---

## Gold (`hop.yaml`)

Gold defaults to `materialized_view` with `read_mode: snapshot`. Dimensions carry the SCD-2 audit columns from silver; facts carry surrogate key references to each dimension.

```yaml
# Dimension
object_name: tbl_dim_{object}
write_mode: materialized_view
read_mode: snapshot
source_schema: silver

primary_key_columns:
  - {object}_skey

# Fact
object_name: tbl_mart_{object}
write_mode: materialized_view
read_mode: snapshot
```

---

## Audit Columns by Layer

| Column | Bronze | Silver CDC | Silver SKEY | Silver CONS | Gold |
|---|---|---|---|---|---|
| `__etl_loaded_at` | yes | yes | yes | yes | — |
| `__etl_record_indicator` | — | yes | — | yes | — |
| `__etl_fprint` | — | yes | — | — | — |
| `__etl_effective_from` | — | SCD-2 only | SCD-2 only | SCD-2 only | inherited |
| `__etl_effective_to` | — | SCD-2 only | — | SCD-2 only | inherited |
| `__etl_is_current` | — | SCD-2 only | — | SCD-2 only | inherited |

---

## Naming Conventions

| Layer | Prefix | Example |
|---|---|---|
| Bronze | `tbl_raw_` | `tbl_raw_customer` |
| Silver CDC | `tbl_cdc_` | `tbl_cdc_customer` |
| Silver SKEY | `tbl_skey_` | `tbl_skey_customer` |
| Silver CONS | `tbl_cons_` | `tbl_cons_customer` |
| Gold Dimensional | `tbl_dim_` | `tbl_dim_customer` |
| Gold Data Mart | `tbl_mart_` | `tbl_mart_sales` |

All names: `lower_snake_case`.

---

## When Config-Driven Isn't Enough

Use `custom_sql` when:

- **Skey resolution** — joining CDC to SKEY tables (standard pattern for all CONS objects)
- **Multi-source consolidation** — joins across two or more CDC objects
- **Complex business logic** — window functions, conditional aggregations, or rules that don't reduce to per-column expressions

When `custom_sql` is set, the `columns` block is ignored entirely. Write it as a `SELECT` from `STREAM({source})` for silver streaming tables, or a plain `SELECT` from silver tables for gold snapshot reads.
