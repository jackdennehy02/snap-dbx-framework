# CLAUDE.md — snap-dbx-framework

Before doing any work in this project, read the following documents in order. They contain the architecture decisions, data model, SETL framework context, and config schemas that inform everything here.

---

## Required Reading

### 1. Architecture
**[architecture.md](architecture.md)**
The canonical reference for the pipeline layers, SETL mapping, naming conventions, audit columns, and config inheritance model. If something contradicts this doc, flag it rather than assuming.

### 2. Master Object Registry
**[config/objects.yaml](config/objects.yaml)**
Defines every object in the pipeline — what layers it flows through, its table name at each layer, load behaviour, and whether it's metadata-driven. The generator uses this to produce per-layer config files.

### 3. Config Templates
**[config_template/objects.yaml](config_template/objects.yaml)** — schema and field documentation for the master object registry.
**[config_template/loading.yaml](config_template/loading.yaml)** — schema for bronze ingestion configs (`config/loading/`).
**[config_template/hop.yaml](config_template/hop.yaml)** — schema for silver and gold hop configs (`config/{hop}/`).

### 4. Hop Defaults
**[config_template/hop_defaults.yaml](config_template/hop_defaults.yaml)**
Shared defaults per hop. Object-level configs only need to set fields that differ from these.

### 5. Project Context
**[../../raw_docs/SETL_framework_matillion](../../raw_docs/SETL_framework_matillion)** — Snap's SETL framework documentation. This is the Matillion/Snowflake implementation this project mirrors on Databricks.
**[../../raw_docs/final_target_potential_data_model.txt](../../raw_docs/final_target_potential_data_model.txt)** — Target data model (DBML format). Gold layer table definitions, relationships, and SCD-2 decisions.
**[../../project_brief](../../project_brief)** — Project brief. Scope, goals, and constraints.

---

## Key Decisions to Carry Forward

- **Bronze is append-only.** Every load is retained as raw history. No overwrites.
- **CDC (Silver) does full table comparison.** It compares the current bronze state against what's already in the CDC table and writes only N/C/D records through. Identical records are filtered.
- **SKEY is insert-only.** Once a surrogate key is assigned it never changes. SCD-2 objects generate a new skey per effective version.
- **Naming convention:** `tbl_raw_`, `tbl_cdc_`, `tbl_skey_`, `tbl_cons_`, `tbl_dim_`, `tbl_mart_`. All `lower_snake_case`.
- **objects.yaml is the orchestrator.** It drives generation of all per-layer config files. Don't edit individual layer configs without checking objects.yaml first.
- **Config is metadata-driven by default.** `metadata_driven: false` means the object requires hand-written SQL in Databricks — flag this clearly.
- **Gold is the dimensional model.** Dimensions are SCD-2 for customer and material; plant is SCD-1. Facts carry surrogate keys resolved in the CONS layer.
