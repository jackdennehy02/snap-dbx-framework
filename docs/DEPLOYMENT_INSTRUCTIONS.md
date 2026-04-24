# snap-dbx-framework Deployment Instructions

---

## Prerequisites

Before deploying, ensure you have:

1. **Databricks CLI** installed
   - Download from https://docs.databricks.com/dev-tools/cli/
   - Verify installation: `databricks --version`

2. **A Databricks workspace** with:
   - Admin or workspace owner access
   - Unity Catalog enabled
   - At least one metastore configured

3. **Authentication profile(s)** configured for your target workspace(s)
   - See [Configuring Authentication](#configuring-authentication) below

---

## Configuring Authentication

The Databricks CLI uses authentication profiles stored in `~/.databrickscfg`.

### View existing profiles:
```powershell
databricks auth profiles
```

### Add a new profile:
```powershell
databricks configure --token
```

When prompted, enter:
- **Workspace URL**: `https://your-workspace-url`
- **Token**: Your Databricks personal access token
- **Profile name**: A descriptive name (optional; defaults to DEFAULT)

---

## Creating the Unity Catalog

The framework requires a Unity Catalog — `dev_setl` by default. Create it before deploying.

### Via Databricks UI (recommended)

1. Log in to your workspace
2. Click **Catalog** → **Create catalog**
3. Set the name to `dev_setl`
4. Choose a storage location and click **Create**

### Via Databricks CLI

```powershell
databricks catalogs create dev_setl --profile <profile-name>
```

---

## Configuring the Bundle

The bundle is defined in `databricks.yml`. The key sections to update before deploying:

### Target workspace profile

```yaml
targets:
  dev:
    workspace:
      profile: <your-profile-name>   # matches a profile in ~/.databrickscfg
    variables:
      catalog: dev_setl
```

### Catalog name

If you created a catalog with a different name, update the `catalog` variable:

```yaml
variables:
  catalog: your-catalog-name
```

### Layer configuration

The pipeline architecture is defined in the `configuration` block. The `ev_lNN_location`, `ev_lNN_name`, `ev_lNN_catalog`, and `ev_lNN_schema` variables drive everything — layer names, config folder structure, and where tables are written.

```yaml
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
```

Don't edit generated config files directly — change `databricks.yml` or `objects.yml` and redeploy.

---

## Deploying

### Step 1: Set the environment variable

Databricks Bundles require `DATABRICKS_BUNDLE_ENGINE=direct` when using catalog resources:

```powershell
# Permanent (all new terminals):
setx DATABRICKS_BUNDLE_ENGINE direct

# Current session only:
$env:DATABRICKS_BUNDLE_ENGINE = 'direct'
```

### Step 2: Validate

```powershell
databricks bundle validate --target dev
```

### Step 3: Deploy

```powershell
databricks bundle deploy --target dev
```

Config files are generated automatically during deployment via the DAB Python integration in `resources/config_generator.py`. You don't need a separate generation step.

To generate config files locally without deploying:

```powershell
python resources/config_generator.py
```

---

## What Gets Deployed

### Schemas

Three schemas are created in the catalog:

| Schema | Contents |
|---|---|
| `01_bronze` | Raw landing tables (`raw_*`) |
| `02_silver` | CDC tables (`cdc_*`) and processed tables (`processed_*`) |
| `03_gold` | Consolidated and dimensional tables |

### DLT Pipeline

A Delta Live Tables pipeline named `snap-dbx-framework-pipeline` is created with:
- **Serverless mode**: enabled
- **Configuration**: all ETL environment variables set automatically

### Notebooks

| Notebook | Layer | Purpose |
|---|---|---|
| `src/bronze/raw.py` | Bronze | Append-only raw ingest from source |
| `src/silver/cdc.py` | Silver | Auto CDC — maintains SCD history |
| `src/silver/processed.py` | Silver | Typecasting, cleansing, surrogate key generation |

---

## Troubleshooting

### "Catalog 'dev_setl' does not exist"

Create the catalog first — see [Creating the Unity Catalog](#creating-the-unity-catalog).

### "Catalog resources are only supported with direct deployment mode"

```powershell
setx DATABRICKS_BUNDLE_ENGINE direct
```

Close and reopen your terminal after running this.

### "Metastore storage root URL does not exist"

Your workspace doesn't have a default storage root. Either contact your Databricks admin, or specify a storage location when creating the catalog:

```powershell
databricks catalogs create dev_setl --storage-root "s3://my-bucket/catalogs/dev_setl"
```

### "The target schema field is required for UC pipelines"

Ensure `databricks.yml` includes a `target` field under the pipeline:

```yaml
pipelines:
  snap_dbx_framework:
    target: 01_bronze
```

### Deployment succeeds but pipeline doesn't run

Trigger it manually: **Jobs & Pipelines** → `snap-dbx-framework-pipeline` → **Start**.

---

## Redeploying

```powershell
databricks bundle validate --target dev
databricks bundle deploy --target dev
```

---

## Removing the Deployment

```powershell
databricks bundle destroy --target dev
```

This deletes all schemas, tables, the DLT pipeline, and deployed notebooks. It does **not** delete the catalog itself.
