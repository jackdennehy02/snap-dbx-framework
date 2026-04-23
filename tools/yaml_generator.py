"""
Config generator — reads objects.yml and produces per-object YAML config files
for each enabled, metadata-driven hop.

Catalog and schema defaults are read from databricks.yml for the given target,
so there is no need to pass them explicitly.

Usage:
    python tools/yaml_generator.py
    python tools/yaml_generator.py --target prod
    python tools/yaml_generator.py --config-root config --objects-file objects.yml
"""

import argparse
import os
import re
import yaml

from pathlib import Path

# ── Constants ──────────────────────────────────────────────────────────────────

BUNDLE_ROOT = Path(__file__).parent.parent

# Parent folder per layer — matches the Unity Catalog schema names in databricks.yml.
LAYER_FOLDERS = {
    "bronze": "01_bronze",
    "silver": "02_silver",
}

TEMPLATE_MAP = {
    ("bronze", "l01"): "loading.yml",
    ("bronze", "l02"): "bronze_cdc.yml",
    ("silver", "l03"): "silver_processed.yml",
    ("silver", "l04"): "silver_skey.yml",
}

# Maps each hop to its source hop and the fallback object_name pattern if the
# source hop has no explicit object_name set.
SOURCE_HOP_DEFAULTS = {
    ("bronze", "l02"): (("bronze", "l01"), "{object}"),
    ("silver", "l03"): (("bronze", "l02"), "{object}"),
    ("silver", "l04"): (("silver", "l03"), "{object}"),
}

CONNECTION_SECTION_MARKERS = {
    "cloud_storage":        "# ── cloud_storage",
    "jdbc":                 "# ── jdbc",
    "kafka":                "# ── kafka",
    "lakehouse_federation": "# ── lakehouse_federation",
}

# ── databricks.yml parsing ─────────────────────────────────────────────────────

def load_hop_defaults(target: str) -> dict:
    """
    Read databricks.yml and return catalog/schema defaults per hop for the
    given target. Resolves ${var.catalog} references using the target's
    variable overrides.
    """
    config_path = BUNDLE_ROOT / "databricks.yml"
    with open(config_path) as f:
        bundle = yaml.safe_load(f)

    global_vars = bundle.get("variables", {})
    target_vars = bundle.get("targets", {}).get(target, {}).get("variables", {})

    catalog = (
        target_vars.get("catalog")
        or global_vars.get("catalog", {}).get("default", "")
    )

    pipelines = bundle.get("resources", {}).get("pipelines", {})
    pipeline_config = next(iter(pipelines.values()), {}).get("configuration", {})

    def resolve(value):
        if not isinstance(value, str):
            return str(value) if value is not None else ""
        return value.replace("${var.catalog}", catalog)

    return {
        ("bronze", "l01"): {"catalog": catalog, "schema": resolve(pipeline_config.get("ev_l01_schema", "")), "name": pipeline_config.get("ev_l01_name", "")},
        ("bronze", "l02"): {"catalog": catalog, "schema": resolve(pipeline_config.get("ev_l02_schema", "")), "name": pipeline_config.get("ev_l02_name", "")},
        ("silver", "l03"): {"catalog": catalog, "schema": resolve(pipeline_config.get("ev_l03_schema", "")), "name": pipeline_config.get("ev_l03_name", "")},
        ("silver", "l04"): {"catalog": catalog, "schema": resolve(pipeline_config.get("ev_l04_schema", "")), "name": pipeline_config.get("ev_l04_name", "")},
    }

# ── Template loading & processing ─────────────────────────────────────────────

def load_template(template_dir: str, template_name: str) -> str:
    path = Path(template_dir) / template_name
    with open(path) as f:
        return f.read()


def load_all_templates(template_dir: str) -> dict:
    template_names = set(TEMPLATE_MAP.values())
    return {name: load_template(template_dir, name) for name in template_names}


def strip_irrelevant_connections(template: str, source_type: str) -> str:
    """
    Keep only the connection block matching source_type and remove all others.
    """
    lines = template.splitlines()
    section_order = []

    for i, line in enumerate(lines):
        stripped = line.strip()
        for stype, marker in CONNECTION_SECTION_MARKERS.items():
            if stripped.startswith(marker):
                section_order.append((stype, i))
                break

    section_ranges = {}
    for idx, (stype, start) in enumerate(section_order):
        end = section_order[idx + 1][1] - 1 if idx + 1 < len(section_order) else len(lines) - 1
        while end > start and lines[end].strip() == "":
            end -= 1
        section_ranges[stype] = (start, end)

    exclude_ranges = set()
    for stype, (start, end) in section_ranges.items():
        if stype != source_type:
            actual_end = end + 1 if end + 1 < len(lines) and lines[end + 1].strip() == "" else end
            exclude_ranges.update(range(start, actual_end + 1))

    result = []
    for i, line in enumerate(lines):
        if i in exclude_ranges:
            continue
        if "Populate the block matching your source_type" in line.strip():
            continue
        if source_type in section_ranges:
            keep_start, keep_end = section_ranges[source_type]
            if keep_start < i <= keep_end:
                line = _uncomment_line(line)
        result.append(line)

    return "\n".join(result)


def _uncomment_line(line: str) -> str:
    match = re.match(r'^(\s*)# (.+)$', line)
    if match:
        indent, content = match.group(1), match.group(2)
        if content.strip().startswith("──") or content.strip().startswith("streaming-only"):
            return line
        return f"{indent}{content}"
    return line


def _substitute_fields(content: str, substitutions: dict) -> str:
    lines = content.splitlines()
    result = []
    matched_keys = set()

    for line in lines:
        replaced = False
        for key, value in substitutions.items():
            if key in matched_keys:
                continue
            match = re.match(rf'^(\s*{re.escape(key)}:)\s*(#.*)?$', line)
            if match:
                prefix = match.group(1)
                result.append(f"{prefix} {value}" if value else line)
                matched_keys.add(key)
                replaced = True
                break
        if not replaced:
            result.append(line)

    return "\n".join(result)


def _resolve_source_object(object_key: str, layer: str, sub_layer: str, obj_config: dict, hop_config: dict, hop_defaults: dict) -> str:
    """Return the source object name for a hop.

    Prefers an explicit source_object on the hop. Otherwise builds
    {source_hop_name}_{source_hop_object_name} using the ev_lXX_name from
    databricks.yml as the prefix.
    """
    if hop_config.get("source_object"):
        return hop_config["source_object"]
    entry = SOURCE_HOP_DEFAULTS.get((layer, sub_layer))
    if not entry:
        return ""
    (src_layer, src_sub), fallback_pattern = entry
    src_hop = (obj_config.get(src_layer) or {}).get(src_sub) or {}
    src_object_name = src_hop.get("object_name") or fallback_pattern.replace("{object}", object_key)
    src_name = hop_defaults.get((src_layer, src_sub), {}).get("name", "")
    return f"{src_name}_{src_object_name}" if src_name else src_object_name


_SECTION_HEADER = re.compile(r'^\s*# ── \w')

def _strip_template_header(content: str) -> str:
    """Remove the leading comment block (file path + description) from generated output.

    Stops at the first section header (# ── Label ──) or the first real YAML line,
    whichever comes first, so structural section dividers are preserved.
    """
    lines = content.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if _SECTION_HEADER.match(line):
            break
        if not line.strip() or line.strip().startswith('#'):
            i += 1
        else:
            break
    return "\n".join(lines[i:])


def _inject_yaml_list(content: str, field_name: str, values: list) -> str:
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
            while i < len(lines) and re.match(r'^\s*#\s*-\s*', lines[i]):
                i += 1
            indent = re.match(r'^(\s*)', line).group(1) + "  "
            for v in values:
                result.append(f"{indent}- {v}")
        else:
            result.append(line)
            i += 1
    return "\n".join(result)

# ── Populate functions ─────────────────────────────────────────────────────────

def populate_loading_template(template, object_key, hop_config, top_level, hop_defaults):
    source_type = top_level.get("source_type") or ""
    hop_name = hop_defaults.get(("bronze", "l01"), {}).get("name", "")
    object_name = f"{hop_name}_{hop_config.get('object_name') or f'raw_{object_key}'}" if hop_name else hop_config.get("object_name") or f"raw_{object_key}"
    enabled = hop_config.get("enabled") if hop_config.get("enabled") is not None else True

    content = strip_irrelevant_connections(template, source_type)
    content = _substitute_fields(content, {
        "source_type": source_type,
        "object_name": object_name,
        "catalog":     top_level["catalog"],
        "schema":      top_level["schema"],
        "enabled":     str(enabled).lower(),
    })
    return _inject_yaml_list(content, "primary_key_columns", top_level.get("primary_key_columns", []))


def populate_cdc_template(template, object_key, hop_config, top_level, obj_config, hop_defaults):
    hop_name = hop_defaults.get(("bronze", "l02"), {}).get("name", "")
    object_name = f"{hop_name}_{hop_config.get('object_name') or f'cdc_{object_key}'}" if hop_name else hop_config.get("object_name") or f"cdc_{object_key}"
    source_object = _resolve_source_object(object_key, "bronze", "l02", obj_config, hop_config, hop_defaults)
    enabled = hop_config.get("enabled") if hop_config.get("enabled") is not None else True

    content = _substitute_fields(template, {
        "source_object": source_object,
        "object_name":   object_name,
        "enabled":       str(enabled).lower(),
        "catalog":       top_level["catalog"],
        "schema":        top_level["schema"],
    })
    return _inject_yaml_list(content, "keys", top_level.get("primary_key_columns", []))


def populate_processed_template(template, object_key, hop_config, top_level, obj_config, hop_defaults):
    hop_name = hop_defaults.get(("silver", "l03"), {}).get("name", "")
    object_name = f"{hop_name}_{hop_config.get('object_name') or f'processed_{object_key}'}" if hop_name else hop_config.get("object_name") or f"processed_{object_key}"
    source_object = _resolve_source_object(object_key, "silver", "l03", obj_config, hop_config, hop_defaults)
    enabled = hop_config.get("enabled") if hop_config.get("enabled") is not None else True

    content = _substitute_fields(template, {
        "source_object": source_object,
        "object_name":   object_name,
        "enabled":       str(enabled).lower(),
        "catalog":       top_level["catalog"],
        "schema":        top_level["schema"],
    })
    return _inject_yaml_list(content, "primary_key_columns", top_level.get("primary_key_columns", []))


def populate_skey_template(template, object_key, hop_config, top_level, obj_config, hop_defaults):
    hop_name = hop_defaults.get(("silver", "l04"), {}).get("name", "")
    base_name = hop_config.get("object_name") or f"skey_{object_key}"
    object_name = f"{hop_name}_{base_name}" if hop_name else base_name
    source_object = _resolve_source_object(object_key, "silver", "l04", obj_config, hop_config, hop_defaults)
    skey_column = hop_config.get("skey_column") or f"{object_key}_skey"
    enabled = hop_config.get("enabled") if hop_config.get("enabled") is not None else True

    content = _substitute_fields(template, {
        "source_object":  source_object,
        "object_name":    object_name,
        "skey_column":    skey_column,
        "enabled":        str(enabled).lower(),
        "catalog":        top_level["catalog"],
        "schema":         top_level["schema"],
    })
    return _inject_yaml_list(content, "business_key_columns", top_level.get("primary_key_columns", []))


def populate_hop_template(template, _object_key, _layer, _sub_layer, hop_config, top_level):
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


def _dispatch_populate(template, object_key, layer, sub_layer, hop_config, top_level, hop_defaults, obj_config):
    layer_defaults = hop_defaults.get((layer, sub_layer), {})
    top_level = {
        **top_level,
        "catalog": hop_config.get("catalog") or layer_defaults.get("catalog", ""),
        "schema":  hop_config.get("schema")  or layer_defaults.get("schema",  ""),
    }
    if layer == "bronze" and sub_layer == "l01":
        return populate_loading_template(template, object_key, hop_config, top_level, hop_defaults)
    elif layer == "bronze" and sub_layer == "l02":
        return populate_cdc_template(template, object_key, hop_config, top_level, obj_config, hop_defaults)
    elif layer == "silver" and sub_layer == "l03":
        return populate_processed_template(template, object_key, hop_config, top_level, obj_config, hop_defaults)
    elif layer == "silver" and sub_layer == "l04":
        return populate_skey_template(template, object_key, hop_config, top_level, obj_config, hop_defaults)
    else:
        return populate_hop_template(template, object_key, layer, sub_layer, hop_config, top_level)

# ── Generator ──────────────────────────────────────────────────────────────────

def load_objects(config_root: str, objects_file: str) -> dict:
    objects_path = Path(config_root) / objects_file
    with open(objects_path) as f:
        data = yaml.safe_load(f)
    return data.get("objects", {})


def generate_configs(config_root: str, objects_file: str, hop_defaults: dict):
    root = Path(config_root)
    template_dir = root / "config_templates"
    objects = load_objects(config_root, objects_file)

    if not objects:
        print("No objects found in objects.yml — nothing to generate.")
        return

    templates = load_all_templates(str(template_dir))

    print(f"Config root:  {config_root}")
    print(f"Objects file: {objects_file}")
    print()

    created = []
    skipped = []

    for object_key, obj_config in objects.items():
        if not obj_config:
            print(f"  WARNING: '{object_key}' has no configuration — skipping.")
            continue

        top_level = {
            "object_alias":        obj_config.get("object_alias"),
            "source_type":         obj_config.get("source_type"),
            "primary_key_columns": obj_config.get("primary_key_columns") or [],
        }

        for layer in ("bronze", "silver"):
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
                template_name = TEMPLATE_MAP.get(folder_key)

                hop_name = hop_defaults.get(folder_key, {}).get("name")
                layer_folder = LAYER_FOLDERS.get(layer)

                if not hop_name or not layer_folder:
                    print(f"  WARNING: Unknown hop ({layer}, {sub_layer}) for '{object_key}' — skipping.")
                    continue
                if not template_name:
                    print(f"  WARNING: No template mapped for ({layer}, {sub_layer}) — skipping.")
                    continue

                folder_path = root / layer_folder / hop_name
                file_path = folder_path / f"{hop_name}_{object_key}.yml"

                folder_path.mkdir(parents=True, exist_ok=True)

                if file_path.exists():
                    skipped.append(str(file_path))
                    continue

                template = templates[template_name]
                content = _dispatch_populate(template, object_key, layer, sub_layer, hop_config, top_level, hop_defaults, obj_config)
                content = _strip_template_header(content)

                with open(file_path, "w") as f:
                    f.write(content)

                created.append(str(file_path))

    print(f"Created: {len(created)}  |  Skipped (already exist): {len(skipped)}")

    if created:
        print("\nNew files:")
        for fp in created:
            print(f"  + {fp}")

    if skipped:
        print("\nSkipped:")
        for fp in skipped:
            print(f"  ~ {fp}")

# ── Entry point ────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description="Generate per-object config files from objects.yml.")
    parser.add_argument(
        "--target",
        default="dev",
        help="Databricks target from databricks.yml (default: dev)",
    )
    parser.add_argument(
        "--config-root",
        default=str(BUNDLE_ROOT / "config"),
        help="Path to the config directory (default: <bundle_root>/config)",
    )
    parser.add_argument(
        "--objects-file",
        default="objects.yml",
        help="Objects registry filename relative to config-root (default: objects.yml)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    hop_defaults = load_hop_defaults(args.target)
    generate_configs(args.config_root, args.objects_file, hop_defaults)
