# Databricks notebook source
# MAGIC %md
# MAGIC # Config Generator
# MAGIC Reads `objects.yml` and generates per-object YAML config files
# MAGIC for each enabled, metadata-driven hop.
# MAGIC
# MAGIC Templates are read from `config_templates/` — this script never
# MAGIC hard-codes template content.
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

# Maps (layer, sub_layer) -> folder path under config root
HOP_FOLDERS = {
    ("bronze", "raw"):              "bronze/raw",
    ("bronze", "cdc"):              "bronze/cdc",
    ("silver", "processed"):        "silver/processed",
    ("silver", "skey"):             "silver/skey",
    ("silver", "consolidation"):    "silver/consolidation",
    ("gold", "dimensional"):        "gold/dimensional",
    ("gold", "data_mart"):          "gold/data_mart",
}

# Default object name prefixes per hop
OBJECT_NAME_PREFIXES = {
    ("bronze", "raw"):              "tbl_raw_",
    ("bronze", "cdc"):              "tbl_cdc_",
    ("silver", "processed"):        "tbl_proc_",
    ("silver", "skey"):             "tbl_skey_",
    ("silver", "consolidation"):    "tbl_cons_",
    ("gold", "dimensional"):        "tbl_dim_",
    ("gold", "data_mart"):          "tbl_mart_",
}

# Connection section markers in the loading template.
# Each value is the comment prefix that starts that block.
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


def strip_irrelevant_connections(template: str, source_type: str) -> str:
    """
    Given the full loading template and a source_type, keep only the
    matching connection block and remove all others.

    Also strips the generic 'Populate the block matching your source_type'
    comment and uncomments the kept section if it was commented out.

    Works by finding each '# ── <type>' section marker and collecting
    lines until the next marker or end of file.
    """
    lines = template.splitlines()
    result = []

    # First pass: identify the line ranges for each connection section
    section_ranges = {}  # source_type -> (start_line, end_line) inclusive
    section_order = []

    for i, line in enumerate(lines):
        stripped = line.strip()
        for stype, marker in CONNECTION_SECTION_MARKERS.items():
            if stripped.startswith(marker):
                section_order.append((stype, i))
                break

    # Calculate end of each section (up to but not including next section start)
    for idx, (stype, start) in enumerate(section_order):
        if idx + 1 < len(section_order):
            end = section_order[idx + 1][1] - 1
        else:
            end = len(lines) - 1
        # Trim trailing blank lines from the section
        while end > start and lines[end].strip() == "":
            end -= 1
        section_ranges[stype] = (start, end)

    # Determine which line ranges to exclude
    exclude_ranges = set()
    for stype, (start, end) in section_ranges.items():
        if stype != source_type:
            # Also exclude any blank lines between this section and the next
            actual_end = end
            if end + 1 < len(lines) and lines[end + 1].strip() == "":
                actual_end = end + 1
            for i in range(start, actual_end + 1):
                exclude_ranges.add(i)

    # Second pass: build output
    for i, line in enumerate(lines):
        if i in exclude_ranges:
            continue

        stripped = line.strip()

        # Remove the generic instruction comment
        if "Populate the block matching your source_type" in stripped:
            continue

        # Uncomment lines in the kept connection section
        if source_type in section_ranges:
            keep_start, keep_end = section_ranges[source_type]
            if keep_start < i <= keep_end:
                line = _uncomment_line(line)

        result.append(line)

    return "\n".join(result)


def _uncomment_line(line: str) -> str:
    """
    Uncomment a YAML line: '  # key: value' -> '  key: value'.
    Only removes leading '# ' from content lines, not section headers.
    """
    match = re.match(r'^(\s*)# (.+)$', line)
    if match:
        indent = match.group(1)
        content = match.group(2)
        # Don't uncomment section headers or sub-section comments
        if content.strip().startswith("──") or content.strip().startswith("streaming-only"):
            return line
        return f"{indent}{content}"
    return line


def _substitute_fields(content: str, substitutions: dict) -> str:
    """
    For each key in substitutions, find the YAML line 'key:' or
    'key:  # comment' and set the value.

    Only matches the first occurrence of each key to avoid
    clobbering nested keys with the same name.
    Preserves inline comments when no value is set.
    """
    lines = content.splitlines()
    result = []
    matched_keys = set()

    for line in lines:
        replaced = False
        for key, value in substitutions.items():
            if key in matched_keys:
                continue
            # Match: 'key:' optionally followed by whitespace and/or comment
            pattern = rf'^(\s*{re.escape(key)}:)\s*(#.*)?$'
            match = re.match(pattern, line)
            if match:
                prefix = match.group(1)
                if value:
                    result.append(f"{prefix} {value}")
                else:
                    # No value — keep original line (blank + comment)
                    result.append(line)
                matched_keys.add(key)
                replaced = True
                break

        if not replaced:
            result.append(line)

    return "\n".join(result)


def populate_loading_template(
    template: str,
    object_key: str,
    hop_config: dict,
    top_level: dict,
) -> str:
    """
    Take the loading template, strip irrelevant connection blocks,
    and populate known fields from objects.yml.
    """
    source_type = top_level.get("source_type") or ""
    object_name = hop_config.get("object_name") or f"tbl_raw_{object_key}"
    catalog = hop_config.get("catalog") or ""
    schema = hop_config.get("schema") or ""
    enabled = hop_config.get("enabled") if hop_config.get("enabled") is not None else True

    # Strip irrelevant connection sections
    content = strip_irrelevant_connections(template, source_type)

    # Field substitutions
    substitutions = {
        "source_type": source_type,
        "object_name": object_name,
        "catalog": catalog,
        "schema": schema,
        "enabled": str(enabled).lower(),
    }

    content = _substitute_fields(content, substitutions)
    return content


def populate_hop_template(
    template: str,
    object_key: str,
    layer: str,
    sub_layer: str,
    hop_config: dict,
    top_level: dict,
) -> str:
    """
    Take the hop template and populate known fields from objects.yml.
    """
    prefix = OBJECT_NAME_PREFIXES.get((layer, sub_layer), f"tbl_{sub_layer}_")
    object_name = hop_config.get("object_name") or f"{prefix}{object_key}"
    catalog = hop_config.get("catalog") or ""
    schema = hop_config.get("schema") or ""
    enabled = hop_config.get("enabled") if hop_config.get("enabled") is not None else True
    object_alias = top_level.get("object_alias") or ""

    substitutions = {
        "object_name": object_name,
        "object_alias": object_alias,
        "catalog": catalog,
        "schema": schema,
        "enabled": str(enabled).lower(),
    }

    content = _substitute_fields(template, substitutions)
    return content

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
    Main generator. For each object in objects.yml, creates
    hop folders and YAML config files where metadata_driven is true.
    Skips any file that already exists (idempotent).
    """
    root = Path(config_root)
    objects = load_objects(config_root, objects_file)

    if not objects:
        print("⚠️  No objects found in objects.yml — nothing to generate.")
        return

    # Load templates once
    loading_template = load_template(template_dir, "loading.yml")
    hop_template = load_template(template_dir, "hop.yml")

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
            "object_alias": obj_config.get("object_alias"),
            "source_type": obj_config.get("source_type"),
        }

        # Walk each layer -> sub-layer
        for layer in ("bronze", "silver", "gold"):
            layer_config = obj_config.get(layer)
            if not layer_config:
                continue

            for sub_layer, hop_config in layer_config.items():
                if not hop_config:
                    continue

                # Skip if explicitly disabled
                if hop_config.get("enabled") is False:
                    continue

                # Skip if not metadata-driven
                if not hop_config.get("metadata_driven"):
                    continue

                # Determine folder and file path
                folder_key = (layer, sub_layer)
                folder_rel = HOP_FOLDERS.get(folder_key)
                if not folder_rel:
                    print(f"⚠️  Unknown hop ({layer}, {sub_layer}) — skipping.")
                    continue

                folder_path = root / folder_rel
                file_path = folder_path / f"{object_key}.yml"

                # Ensure the directory exists
                ensure_directory(folder_path)

                # Idempotent: skip if file already exists
                if file_path.exists():
                    skipped.append(str(file_path))
                    continue

                # Generate the appropriate template
                if layer == "bronze" and sub_layer == "raw":
                    content = populate_loading_template(
                        loading_template, object_key, hop_config, top_level
                    )
                else:
                    content = populate_hop_template(
                        hop_template, object_key, layer, sub_layer, hop_config, top_level
                    )

                # Write the file
                with open(file_path, "w") as f:
                    f.write(content)

                created.append(str(file_path))

    # Summary
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