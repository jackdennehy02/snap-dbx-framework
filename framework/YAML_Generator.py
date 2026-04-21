# Databricks notebook source
# MAGIC %md
# MAGIC # Config Generator
# MAGIC Reads `objects.yml` and generates per-object YAML config files
# MAGIC for each enabled, metadata-driven hop.
# MAGIC
# MAGIC Each layer has its own template in `config_templates/` — this
# MAGIC script never hard-codes template content.
# MAGIC
# MAGIC **Idempotent** — existing files are never overwritten.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

# dbutils.widgets.text("project_root", "/Workspace/Users/jack.dennehy@snapanalytics.co.uk/snap-academy-internal-project/snap-dbx-framework", "Project root path")
# dbutils.widgets.text("objects_file", "objects.yml", "Objects registry filename (relative to config/)")

# Uncomment the above in Databricks. For local testing, use defaults:
import os

PROJECT_ROOT = os.environ.get(
    "PROJECT_ROOT",
    "/Workspace/Users/jack.dennehy@snapanalytics.co.uk/snap-academy-internal-project/snap-dbx-framework",
)
OBJECTS_FILE = os.environ.get("OBJECTS_FILE", "objects.yml")

# In Databricks, replace the above with:
# PROJECT_ROOT = dbutils.widgets.get("project_root")
# OBJECTS_FILE = dbutils.widgets.get("objects_file")

# Derived paths
CONFIG_ROOT = os.path.join(PROJECT_ROOT, "config")
TEMPLATE_DIR = os.path.join(PROJECT_ROOT, "config_templates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports & Setup

# COMMAND ----------

import re
import yaml
from pathlib import Path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Naming Convention & Folder Structure

# COMMAND ----------

# Maps (layer, sub_layer) -> ordered folder path under config root
HOP_FOLDERS = {
    ("bronze", "raw"):              "01_bronze/01a_raw",
    ("bronze", "cdc"):              "01_bronze/01b_cdc",
    ("silver", "processed"):        "02_silver/02a_processed",
    ("silver", "skey"):             "02_silver/02b_skey",
    ("silver", "consolidation"):    "02_silver/02c_consolidation",
    ("gold", "dimensional"):        "03_gold/03a_dimensional",
    ("gold", "data_mart"):          "03_gold/03b_data_mart",
}

# Maps (layer, sub_layer) -> template filename in config_templates/
TEMPLATE_MAP = {
    ("bronze", "raw"):              "loading.yml",
    ("bronze", "cdc"):              "bronze_cdc.yml",
    ("silver", "processed"):        "silver_processed.yml",
    ("silver", "skey"):             "silver_skey.yml",
    ("silver", "consolidation"):    "hop.yml",
    ("gold", "dimensional"):        "hop.yml",
    ("gold", "data_mart"):          "hop.yml",
}

# Default object name prefixes per hop (bronze/silver only — consolidation onwards requires explicit object_name in objects.yml)
OBJECT_NAME_PREFIXES = {
    ("bronze", "raw"):              "tbl_a_raw_",
    ("bronze", "cdc"):              "tbl_b_cdc_",
    ("silver", "processed"):        "tbl_a_proc_",
    ("silver", "skey"):             "tbl_b_skey_",
}

# Hops where object_name must be explicitly set in objects.yml — no auto-generated fallback
REQUIRES_EXPLICIT_OBJECT_NAME = {
    ("silver", "consolidation"),
    ("gold", "dimensional"),
    ("gold", "data_mart"),
}

# Default source object for each hop (what it reads from upstream)
SOURCE_OBJECT_DEFAULTS = {
    ("bronze", "cdc"):              "tbl_a_raw_{object}",
    ("silver", "processed"):        "tbl_b_cdc_{object}",
    ("silver", "skey"):             "tbl_a_proc_{object}",
}

# Default catalog and schema per layer — overridden by hop_config or objects.yml if set
LAYER_DEFAULTS = {
    "bronze": {"catalog": "snap_dbx", "schema": "01_bronze"},
    "silver": {"catalog": "snap_dbx", "schema": "02_silver"},
    "gold":   {"catalog": "snap_dbx", "schema": "03_gold"},
}

# Connection section markers in the loading template
CONNECTION_SECTION_MARKERS = {
    "cloud_storage":        "# ── cloud_storage",
    "jdbc":                 "# ── jdbc",
    "kafka":                "# ── kafka",
    "lakehouse_federation": "# ── lakehouse_federation",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Template Loading & Processing

# COMMAND ----------

def load_template(template_dir: str, template_name: str) -> str:
    """Read a template file from the config_templates directory."""
    path = Path(template_dir) / template_name
    with open(path, "r") as f:
        return f.read()


def load_all_templates(template_dir: str) -> dict:
    """Load all required templates once and return as a dict keyed by filename."""
    template_names = set(TEMPLATE_MAP.values())
    return {name: load_template(template_dir, name) for name in template_names}


def strip_irrelevant_connections(template: str, source_type: str) -> str:
    """
    Given the full loading template and a source_type, keep only the
    matching connection block and remove all others.
    """
    lines = template.splitlines()
    result = []

    section_ranges = {}
    section_order = []

    for i, line in enumerate(lines):
        stripped = line.strip()
        for stype, marker in CONNECTION_SECTION_MARKERS.items():
            if stripped.startswith(marker):
                section_order.append((stype, i))
                break

    for idx, (stype, start) in enumerate(section_order):
        if idx + 1 < len(section_order):
            end = section_order[idx + 1][1] - 1
        else:
            end = len(lines) - 1
        while end > start and lines[end].strip() == "":
            end -= 1
        section_ranges[stype] = (start, end)

    exclude_ranges = set()
    for stype, (start, end) in section_ranges.items():
        if stype != source_type:
            actual_end = end
            if end + 1 < len(lines) and lines[end + 1].strip() == "":
                actual_end = end + 1
            for i in range(start, actual_end + 1):
                exclude_ranges.add(i)

    for i, line in enumerate(lines):
        if i in exclude_ranges:
            continue
        stripped = line.strip()
        if "Populate the block matching your source_type" in stripped:
            continue
        if source_type in section_ranges:
            keep_start, keep_end = section_ranges[source_type]
            if keep_start < i <= keep_end:
                line = _uncomment_line(line)
        result.append(line)

    return "\n".join(result)


def _uncomment_line(line: str) -> str:
    """Uncomment a YAML line: '  # key: value' -> '  key: value'."""
    match = re.match(r'^(\s*)# (.+)$', line)
    if match:
        indent = match.group(1)
        content = match.group(2)
        if content.strip().startswith("──") or content.strip().startswith("streaming-only"):
            return line
        return f"{indent}{content}"
    return line


def _substitute_fields(content: str, substitutions: dict) -> str:
    """
    For each key in substitutions, find the YAML line 'key:' or
    'key:  # comment' and set the value. Only matches the first
    occurrence of each key. Preserves inline comments when no value set.
    """
    lines = content.splitlines()
    result = []
    matched_keys = set()

    for line in lines:
        replaced = False
        for key, value in substitutions.items():
            if key in matched_keys:
                continue
            pattern = rf'^(\s*{re.escape(key)}:)\s*(#.*)?$'
            match = re.match(pattern, line)
            if match:
                prefix = match.group(1)
                if value:
                    result.append(f"{prefix} {value}")
                else:
                    result.append(line)
                matched_keys.add(key)
                replaced = True
                break
        if not replaced:
            result.append(line)

    return "\n".join(result)


def _resolve_source_object(object_key: str, layer: str, sub_layer: str, hop_config: dict) -> str:
    """
    Return the source_object for this hop. Uses hop_config override if set,
    otherwise falls back to the convention defined in SOURCE_OBJECT_DEFAULTS.
    Returns empty string for hops with no default (consolidation, gold).
    """
    if hop_config.get("source_object"):
        return hop_config["source_object"]
    template = SOURCE_OBJECT_DEFAULTS.get((layer, sub_layer), "")
    return template.replace("{object}", object_key)


def _inject_yaml_list(content: str, field_name: str, values: list) -> str:
    """
    Replace a YAML list field's commented-out placeholder lines with actual values.
    No-ops if values is empty.
    """
    if not values:
        return content
    lines = content.splitlines()
    result = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if re.match(rf'^\s*{re.escape(field_name)}:\s*(#.*)?$', line):
            result.append(line)
            i += 1
            # Skip commented placeholder lines (e.g. "  # - col_a")
            while i < len(lines) and re.match(r'^\s*#\s*-\s*', lines[i]):
                i += 1
            indent = re.match(r'^(\s*)', line).group(1) + "  "
            for v in values:
                result.append(f"{indent}- {v}")
        else:
            result.append(line)
            i += 1
    return "\n".join(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Populate Functions (one per template type)

# COMMAND ----------

def populate_loading_template(template, object_key, hop_config, top_level):
    """Bronze raw — loading template."""
    source_type = top_level.get("source_type") or ""
    object_name = hop_config.get("object_name") or f"tbl_a_raw_{object_key}"
    enabled = hop_config.get("enabled") if hop_config.get("enabled") is not None else True

    content = strip_irrelevant_connections(template, source_type)
    content = _substitute_fields(content, {
        "source_type": source_type,
        "object_name": object_name,
        "catalog": top_level["catalog"],
        "schema":  top_level["schema"],
        "enabled": str(enabled).lower(),
    })
    return content


def populate_cdc_template(template, object_key, hop_config, top_level):
    """Bronze CDC — Auto CDC template."""
    object_name = hop_config.get("object_name") or f"tbl_b_cdc_{object_key}"
    source_object = _resolve_source_object(object_key, "bronze", "cdc", hop_config)
    enabled = hop_config.get("enabled") if hop_config.get("enabled") is not None else True

    content = _substitute_fields(template, {
        "source_object": source_object,
        "object_name":   object_name,
        "enabled":       str(enabled).lower(),
        "catalog":       top_level["catalog"],
        "schema":        top_level["schema"],
    })
    return _inject_yaml_list(content, "keys", top_level.get("primary_key_columns", []))


def populate_processed_template(template, object_key, hop_config, top_level):
    """Silver processed — typecasting and basic cleansing."""
    object_name = hop_config.get("object_name") or f"tbl_a_proc_{object_key}"
    source_object = _resolve_source_object(object_key, "silver", "processed", hop_config)
    enabled = hop_config.get("enabled") if hop_config.get("enabled") is not None else True

    content = _substitute_fields(template, {
        "source_object": source_object,
        "object_name":   object_name,
        "enabled":       str(enabled).lower(),
        "catalog":       top_level["catalog"],
        "schema":        top_level["schema"],
    })
    return _inject_yaml_list(content, "primary_key_columns", top_level.get("primary_key_columns", []))


def populate_skey_template(template, object_key, hop_config, top_level):
    """Silver SKEY — surrogate key mapping."""
    object_name = hop_config.get("object_name") or f"tbl_b_skey_{object_key}"
    source_object = _resolve_source_object(object_key, "silver", "skey", hop_config)
    skey_column = hop_config.get("skey_column") or f"{object_key}_skey"
    enabled = hop_config.get("enabled") if hop_config.get("enabled") is not None else True

    content = _substitute_fields(template, {
        "source_object": source_object,
        "object_name":   object_name,
        "skey_column":   skey_column,
        "enabled":       str(enabled).lower(),
        "catalog":       top_level["catalog"],
        "schema":        top_level["schema"],
    })
    return _inject_yaml_list(content, "business_key_columns", top_level.get("primary_key_columns", []))


def populate_hop_template(template, _object_key, _layer, _sub_layer, hop_config, top_level):
    """Standard hop template — consolidation and gold."""
    object_name = hop_config.get("object_name") or ""
    enabled = hop_config.get("enabled") if hop_config.get("enabled") is not None else True

    content = _substitute_fields(template, {
        "object_name":  object_name,
        "object_alias": top_level.get("object_alias") or "",
        "enabled":      str(enabled).lower(),
        "catalog":      top_level["catalog"],
        "schema":       top_level["schema"],
    })
    return _inject_yaml_list(content, "primary_key_columns", top_level.get("primary_key_columns", []))


def _dispatch_populate(template, object_key, layer, sub_layer, hop_config, top_level):
    """Route to the correct populate function based on layer/sub_layer."""
    layer_defaults = LAYER_DEFAULTS.get(layer, {})
    top_level = {
        **top_level,
        "catalog": hop_config.get("catalog") or layer_defaults.get("catalog", ""),
        "schema":  hop_config.get("schema")  or layer_defaults.get("schema",  ""),
    }
    if layer == "bronze" and sub_layer == "raw":
        return populate_loading_template(template, object_key, hop_config, top_level)
    elif layer == "bronze" and sub_layer == "cdc":
        return populate_cdc_template(template, object_key, hop_config, top_level)
    elif layer == "silver" and sub_layer == "processed":
        return populate_processed_template(template, object_key, hop_config, top_level)
    elif layer == "silver" and sub_layer == "skey":
        return populate_skey_template(template, object_key, hop_config, top_level)
    else:
        return populate_hop_template(template, object_key, layer, sub_layer, hop_config, top_level)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generator Logic

# COMMAND ----------

def load_objects(config_root: str, objects_file: str) -> dict:
    """Load and parse the objects.yml registry."""
    objects_path = Path(config_root) / objects_file
    with open(objects_path, "r") as f:
        data = yaml.safe_load(f)
    return data.get("objects", {})


def ensure_directory(path: Path):
    """Create directory if it doesn't exist."""
    path.mkdir(parents=True, exist_ok=True)


def generate_configs(config_root: str, template_dir: str, objects_file: str):
    """
    Main generator. For each object in objects.yml, creates hop folders
    and YAML config files where enabled and metadata_driven are true.
    Skips any file that already exists (idempotent).
    """
    root = Path(config_root)
    objects = load_objects(config_root, objects_file)

    if not objects:
        print("⚠️  No objects found in objects.yml — nothing to generate.")
        return

    templates = load_all_templates(template_dir)

    print(f"📂 Config root:    {config_root}")
    print(f"📄 Templates from: {template_dir}")
    print(f"📋 Objects file:   {objects_file}")

    created = []
    skipped = []

    for object_key, obj_config in objects.items():
        if not obj_config:
            print(f"⚠️  Object '{object_key}' has no configuration — skipping.")
            continue

        top_level = {
            "object_alias":         obj_config.get("object_alias"),
            "source_type":          obj_config.get("source_type"),
            "primary_key_columns":  obj_config.get("primary_key_columns") or [],
        }

        for layer in ("bronze", "silver", "gold"):
            layer_config = obj_config.get(layer)
            if not layer_config:
                continue

            for sub_layer, hop_config in layer_config.items():
                if not hop_config:
                    continue
                if hop_config.get("enabled") is False:
                    continue
                if not hop_config.get("metadata_driven"):
                    continue

                folder_key = (layer, sub_layer)

                if folder_key in REQUIRES_EXPLICIT_OBJECT_NAME and not hop_config.get("object_name"):
                    print(f"⚠️  '{object_key}' ({layer}/{sub_layer}) has no object_name set — skipping. Set it explicitly in objects.yml.")
                    continue

                folder_rel = HOP_FOLDERS.get(folder_key)
                if not folder_rel:
                    print(f"⚠️  Unknown hop ({layer}, {sub_layer}) for '{object_key}' — skipping.")
                    continue

                template_name = TEMPLATE_MAP.get(folder_key)
                if not template_name:
                    print(f"⚠️  No template mapped for ({layer}, {sub_layer}) — skipping.")
                    continue

                folder_path = root / folder_rel
                file_path = folder_path / f"{object_key}.yml"

                ensure_directory(folder_path)

                if file_path.exists():
                    skipped.append(str(file_path))
                    continue

                template = templates[template_name]
                content = _dispatch_populate(template, object_key, layer, sub_layer, hop_config, top_level)

                with open(file_path, "w") as f:
                    f.write(content)

                created.append(str(file_path))

    print(f"\n{'='*60}")
    print(f"Config generation complete")
    print(f"{'='*60}")
    print(f"  Created: {len(created)}")
    print(f"  Skipped (already exist): {len(skipped)}")

    if created:
        print(f"\n📄 New files:")
        for fp in created:
            print(f"    + {fp}")

    if skipped:
        print(f"\n⏭️  Skipped files:")
        for fp in skipped:
            print(f"    ~ {fp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run

# COMMAND ----------

generate_configs(CONFIG_ROOT, TEMPLATE_DIR, OBJECTS_FILE)
