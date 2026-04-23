# snap-dbx-framework Deployment Instructions

This document provides detailed instructions for deploying the snap-dbx-framework to Databricks.

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

The Databricks CLI uses authentication profiles stored in `~/.databrickscfg`. Each profile corresponds to a workspace.

### View existing profiles:
```powershell
databricks auth profiles
```

### Add a new profile:
```powershell
databricks configure --token
```

When prompted, enter:
- **Workspace URL**: `https://your-workspace-url` (e.g., `https://dbc-ef64bebd-146c.cloud.databricks.com`)
- **Token**: Your Databricks personal access token
- **Profile name**: A descriptive name (optional; defaults to DEFAULT)

### Important: Workspace URLs

Each workspace has a unique URL. The bundle deployment will only work with the workspace corresponding to the profile you use. Make sure you're using the correct profile for your target workspace.

To find your workspace URL: log in to Databricks, and look at the URL bar.

---

## Creating the Unity Catalog

The snap-dbx-framework requires a Unity Catalog named `dev_setl` (by default). This must be created in your target workspace before deployment.

### Option 1: Create via Databricks UI (Recommended)

1. Log in to your Databricks workspace
2. Click **Catalog** in the left sidebar
3. Click **Create catalog**
4. Set the name to `dev_setl`
5. For **Storage location**, choose one of:
   - **Use default storage** (if your workspace has default storage configured)
   - **Specify a custom location** (provide an S3 bucket path like `s3://my-bucket/catalogs/dev_setl`)
6. Click **Create**

### Option 2: Create via Databricks CLI

If your workspace has a default storage root configured:

```powershell
databricks catalogs create dev_setl --profile <profile-name>
```

If you need to specify a custom storage location:

```powershell
databricks catalogs create dev_setl --storage-root "s3://my-bucket/catalogs/dev_setl" --profile <profile-name>
```

### Option 3: Create via Python SDK

```python
from databricks.sdk import WorkspaceClient

client = WorkspaceClient(profile="<profile-name>")
catalog = client.catalogs.create(
    name="dev_setl",
    storage_root="s3://my-bucket/catalogs/dev_setl"  # Optional if default storage is configured
)
print(f"Catalog created: {catalog.name}")
```

---

## Configuring the Bundle

The bundle configuration is defined in `databricks.yml`. Before deploying, you may need to update the workspace profile and catalog name.

### Key configuration elements:

#### 1. **Target Workspace Profile**

In `databricks.yml`, locate the `targets.dev` section:

```yaml
targets:
  dev:
    default: true
    workspace:
      profile: <your-profile-name>  # Change this to your profile
    variables:
      catalog: dev_setl
```

Replace `<your-profile-name>` with the profile name from `~/.databrickscfg`. Examples:
- `DEFAULT`
- `dennehy.jack01@gmail.com`
- `staging`

#### 2. **Catalog Name**

If you created a catalog with a different name (not `dev_setl`), update it here:

```yaml
variables:
  catalog: your-catalog-name  # Change this to your catalog name
```

You'll also need to update all references in the `targets.dev` section:

```yaml
variables:
  catalog: your-catalog-name  # Used by pipeline and schemas
```

#### 3. **Configuration Root**

The `config_root` variable points to the framework's config directory. The default is `config`, which is correct for the standard project structure. Only change this if you've moved the config files.

---

## Deploying the Bundle

### Prerequisites for deployment:

- [ ] Databricks CLI is installed and authenticated
- [ ] Target workspace profile is configured in `databricks.yml`
- [ ] `dev_setl` catalog (or your custom catalog) exists in the target workspace
- [ ] You have the environment variable set (see below)

### Step 1: Set the environment variable

Databricks Bundles require the `DATABRICKS_BUNDLE_ENGINE` environment variable set to `direct` when using catalog resources:

#### On Windows (permanent, for all new terminals):
```powershell
setx DATABRICKS_BUNDLE_ENGINE direct
```

Then close and reopen your terminal for the change to take effect.

#### In current terminal session only:
```powershell
$env:DATABRICKS_BUNDLE_ENGINE = 'direct'
```

Verify it's set:
```powershell
echo $env:DATABRICKS_BUNDLE_ENGINE
```

### Step 2: Validate the bundle

Before deploying, validate the configuration:

```powershell
cd snap-dbx-framework
databricks bundle validate --target dev
```

Expected output:
```
Name: snap-dbx-framework
Target: dev
Workspace:
  User: <your-user>
  Path: /Workspace/Users/<your-user>/.bundle/snap-dbx-framework

Validation OK!
```

### Step 3: Deploy

Deploy the bundle to your workspace:

```powershell
databricks bundle deploy --target dev
```

Expected output:
```
Uploading bundle files to /Workspace/Users/<your-user>/.bundle/snap-dbx-framework/files...
Deploying resources...
Updating deployment state...
Deployment complete!
```

---

## What Gets Deployed

When you deploy successfully, the following resources are created in your Databricks workspace:

### 1. **Schemas**

Three schemas are created in the `dev_setl` catalog:
- `dev_setl.01_bronze` – Raw and CDC tables
- `dev_setl.02_silver` – Processed and surrogate key tables
- `dev_setl.03_gold` – Consolidated and dimensional tables

### 2. **DLT Pipeline**

A Delta Live Tables (DLT) pipeline named `snap-dbx-framework-pipeline` is created with:
- **Target**: `dev_setl.01_bronze` (where pipeline outputs are written)
- **Serverless mode**: Enabled (no compute cluster required)
- **Configuration**: All ETL environment variables set up automatically

### 3. **Notebooks**

The following notebooks are uploaded and registered with the pipeline:
- `src/bronze/raw.py` – Raw ingest layer
- `src/bronze/cdc.py` – CDC (change detection) layer
- `src/silver/processed.py` – Data processing and cleansing
- `src/silver/skey.py` – Surrogate key management

---

## Troubleshooting

### Error: "Catalog 'dev_setl' does not exist"

**Cause**: The catalog hasn't been created in your workspace.

**Solution**: 
1. Verify you're in the correct workspace (check the URL and profile)
2. Create the catalog (see [Creating the Unity Catalog](#creating-the-unity-catalog))

### Error: "Catalog resources are only supported with direct deployment mode"

**Cause**: The `DATABRICKS_BUNDLE_ENGINE` environment variable is not set to `direct`.

**Solution**:
```powershell
setx DATABRICKS_BUNDLE_ENGINE direct
```
Then close and reopen your terminal.

### Error: "Metastore storage root URL does not exist"

**Cause**: Your workspace doesn't have a default storage root configured, and you didn't specify one when creating the catalog.

**Solution**:
1. Contact your Databricks admin to configure a default storage root, or
2. Specify a storage location when creating the catalog: `databricks catalogs create dev_setl --storage-root "s3://my-bucket/catalogs/dev_setl"`

### Error: "The target schema field is required for UC pipelines"

**Cause**: The pipeline configuration doesn't specify a target schema.

**Solution**: Ensure `databricks.yml` includes a `target` field:
```yaml
pipelines:
  snap_dbx_framework:
    target: 01_bronze
```

### Error: "unknown command 'sql' for databricks"

**Cause**: You're trying to use an unsupported CLI command.

**Solution**: Use the Databricks UI or Python SDK to create catalogs instead.

### Deployment succeeds but pipeline doesn't run

**Cause**: The pipeline may need to be manually triggered, or there may be missing source data.

**Solution**:
1. Navigate to **Jobs & Pipelines** in your workspace
2. Find `snap-dbx-framework-pipeline`
3. Click **Start** to run the pipeline
4. Check the logs for any errors

---

## Redeploying

If you make changes to the bundle configuration or code:

1. Update the files (e.g., `databricks.yml`, notebooks, config files)
2. Validate: `databricks bundle validate --target dev`
3. Deploy: `databricks bundle deploy --target dev`

The deployment will update existing resources and create new ones as needed.

---

## Removing the Deployment

To remove all resources created by this bundle:

```powershell
databricks bundle destroy --target dev
```

**Warning**: This will delete:
- All schemas and tables in the bundle
- The DLT pipeline
- All deployed notebooks

This **does not** delete the catalog itself (Databricks requires explicit catalog deletion through the UI or API).

---

## Next Steps

Once deployed:

1. Verify the pipeline exists: **Jobs & Pipelines** → `snap-dbx-framework-pipeline`
2. Check the schemas: **Catalog** → `dev_setl` → view `01_bronze`, `02_silver`, `03_gold`
3. Upload source data to your configured ingest location
4. Start the pipeline to begin ETL processing
5. Monitor pipeline runs in the **Runs** tab

---

## Additional Resources

- [Databricks CLI Documentation](https://docs.databricks.com/dev-tools/cli/)
- [Databricks Bundles](https://docs.databricks.com/dev-tools/bundles/)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/)
