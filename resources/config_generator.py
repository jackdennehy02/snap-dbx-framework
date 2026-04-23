"""
Config generator — reads config/objects.yml and writes per-object YAML config
files for each enabled, metadata-driven hop.

Called automatically during `databricks bundle deploy` via the DAB Python entry
point in resources/__init__.py. Can also be run standalone:

    python resources/config_generator.py [--target dev] [--config-root config]
"""

import argparse
import re
import yaml
from pathlib import Path
from typing import Optional

BUNDLE_ROOT = Path(__file__).parent.parent

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

# Maps each hop to its upstream hop for source_object name resolution.
HOP_CHAIN = {
    ("bronze", "l02"): ("bronze", "l01"),
    ("silver", "l03"): ("bronze", "l02"),
    ("silver", "l04"): ("silver", "l03"),
}

CONNECTION_SECTION_MARKERS = {
    "cloud_storage":        "# ── cloud_storage",
    "jdbc":                 "# ── jdbc",
    "kafka":                "# ── kafka",
    "lakehouse_federation": "# ── lakehouse_federation",
}


# ── Config loading ──────────────────────────────────────────────────────────────

def load_hop_defaults(target: str) -> dict:
    """Read catalog/schema/name defaults for each hop from databricks.yml."""
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
        ("bronze", "l01"): {
            "catalog": catalog,
            "schema":  resolve(pipeline_config.get("ev_l01_schema", "")),
            "name":    pipeline_config.get("ev_l01_name", ""),
        },
        ("bronze", "l02"): {
            "catalog": catalog,
            "schema":  resolve(pipeline_config.get("ev_l02_schema", "")),
            "name":    pipeline_config.get("ev_l02_name", ""),
        },
        ("silver", "l03"): {
            "catalog": catalog,
            "schema":  resolve(pipeline_config.get("ev_l03_schema", "")),
            "name":    pipeline_config.get("ev_l03_name", ""),
        },
        ("silver", "l04"): {
            "catalog": catalog,
            "schema":  resolve(pipeline_config.get("ev_l04_schema", "")),
            "name":    pipeline_config.get("ev_l04_name", ""),
        },
    }


def load_source_systems(config_root: str) -> dict:
    """Load source_systems from objects.yml."""
    path = Path(config_root) / "objects.yml"
    with open(path) as f:
        data = yaml.safe_load(f)
    return data.get("source_systems", {})


def parse_object(object_value) -> tuple:
    """
    Normalise an object entry to (primary_key_columns, overrides).

        material: material_key                  → ([material_key], {})
        customer: [customer_id, company_code]   → ([customer_id, company_code], {})
    """
    if isinstance(object_value, str):
        return [object_value], {}
    if isinstance(object_value, list):
        return object_value, {}
    if isinstance(object_value, dict):
        pks = object_value.get("primary_key_columns") or []
        if isinstance(pks, str):
            pks = [pks]
        overrides = {k: v for k, v in object_value.items() if k != "primary_key_columns"}
        return pks, overrides
    return [], {}


def resolve_connection(connection: dict, object_key: str) -> dict:
    """Replace {object} placeholders in connection field values."""
    return {
        k: v.replace("{object}", object_key) if isinstance(v, str) else v
        for k, v in connection.items()
    }


# ── Template processing ─────────────────────────────────────────────────────────

def load_templates(template_dir: str) -> dict:
    names = set(TEMPLATE_MAP.values())
    templates = {}
    for name in names:
        with open(Path(template_dir) / name) as f:
            templates[name] = f.read()
    return templates


def strip_irrelevant_connections(template: str, source_type: str) -> str:
    """Keep only the connection block matching source_type; remove all others."""
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
        end = (
            section_order[idx + 1][1] - 1
            if idx + 1 < len(section_order)
            else len(lines) - 1
        )
        while end > start and lines[end].strip() == "":
            end -= 1
        section_ranges[stype] = (start, end)

    exclude_ranges = set()
    for stype, (start, end) in section_ranges.items():
        if stype != source_type:
            actual_end = (
                end + 1
                if end + 1 < len(lines) and lines[end + 1].strip() == ""
                else end
            )
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


_YAML_NEEDS_QUOTING = re.compile(r'[,:#\[\]{}]')
_YAML_RESERVED = {'true', 'false', 'null', 'yes', 'no', 'on', 'off'}


def _yaml_str(value) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if value is None:
        return ""
    s = str(value)
    if _YAML_NEEDS_QUOTING.search(s) or s.lower() in _YAML_RESERVED:
        return f'"{s}"'
    return s


def _override_field(content: str, key: str, value: str) -> str:
    """Replace a YAML field value regardless of whether it already has one."""
    lines = content.splitlines()
    result = []
    matched = False
    for line in lines:
        if not matched and re.match(rf'^\s*{re.escape(key)}:', line) and not line.lstrip().startswith("#"):
            indent = re.match(r'^(\s*)', line).group(1)
            result.append(f"{indent}{key}: {value}" if value else f"{indent}{key}:")
            matched = True
        else:
            result.append(line)
    return "\n".join(result)


def _substitute_fields(content: str, substitutions: dict) -> str:
    """Fill empty YAML fields from substitutions. Skips fields that already have a value."""
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


_SECTION_HEADER = re.compile(r'^\s*# ── \w')


def _strip_template_header(content: str) -> str:
    """Remove the leading file-path/description comment block; keep section headers."""
    lines = content.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i]
        if _SECTION_HEADER.match(line):
            break
        if not line.strip() or line.strip().startswith("#"):
            i += 1
        else:
            break
    return "\n".join(lines[i:])


def _source_object_name(object_key: str, layer: str, sub_layer: str, hop_defaults: dict) -> str:
    """Derive the source_object name for a hop from its upstream hop."""
    upstream = HOP_CHAIN.get((layer, sub_layer))
    if not upstream:
        return ""
    src_name = hop_defaults.get(upstream, {}).get("name", "")
    return f"{src_name}_{object_key}" if src_name else object_key


# ── Populate functions ──────────────────────────────────────────────────────────

def _populate_loading(template, object_key, hop_config, source_type, connection, primary_keys, hop_defaults):
    hop = hop_defaults[("bronze", "l01")]
    object_name = f"{hop['name']}_{object_key}" if hop["name"] else object_key

    content = strip_irrelevant_connections(template, source_type)
    content = _substitute_fields(content, {
        "source_type":    source_type,
        "object_name":    object_name,
        "catalog":        hop["catalog"],
        "schema":         hop["schema"],
        "ingestion_mode": _yaml_str(hop_config.get("ingestion_mode", "")),
        "merge_strategy": _yaml_str(hop_config.get("merge_strategy", "")),
        **{k: _yaml_str(v) for k, v in connection.items()},
    })
    return _inject_yaml_list(content, "primary_key_columns", primary_keys)


def _populate_cdc(template, object_key, hop_config, primary_keys, hop_defaults):
    hop = hop_defaults[("bronze", "l02")]
    object_name = f"{hop['name']}_{object_key}" if hop["name"] else object_key
    source_object = _source_object_name(object_key, "bronze", "l02", hop_defaults)

    content = _substitute_fields(template, {
        "source_object": source_object,
        "object_name":   object_name,
        "catalog":       hop["catalog"],
        "schema":        hop["schema"],
        "sequence_by":   _yaml_str(hop_config.get("sequence_by", "")),
    })
    content = _override_field(content, "scd_type", _yaml_str(hop_config.get("scd_type", "")))
    return _inject_yaml_list(content, "keys", primary_keys)


def _populate_processed(template, object_key, primary_keys, hop_defaults):
    hop = hop_defaults[("silver", "l03")]
    object_name = f"{hop['name']}_{object_key}" if hop["name"] else object_key
    source_object = _source_object_name(object_key, "silver", "l03", hop_defaults)

    content = _substitute_fields(template, {
        "source_object": source_object,
        "object_name":   object_name,
        "catalog":       hop["catalog"],
        "schema":        hop["schema"],
    })
    return _inject_yaml_list(content, "primary_key_columns", primary_keys)


def _populate_skey(template, object_key, hop_config, primary_keys, hop_defaults):
    hop = hop_defaults[("silver", "l04")]
    object_name = f"{hop['name']}_{object_key}" if hop["name"] else object_key
    source_object = _source_object_name(object_key, "silver", "l04", hop_defaults)

    content = _substitute_fields(template, {
        "source_object": source_object,
        "object_name":   object_name,
        "skey_column":   f"{object_key}_skey",
        "scd_type":      _yaml_str(hop_config.get("scd_type", "")),
        "catalog":       hop["catalog"],
        "schema":        hop["schema"],
    })
    return _inject_yaml_list(content, "business_key_columns", primary_keys)


def _dispatch(template, object_key, layer, sub_layer, hop_config, source_type, connection, primary_keys, hop_defaults):
    if layer == "bronze" and sub_layer == "l01":
        return _populate_loading(template, object_key, hop_config, source_type, connection, primary_keys, hop_defaults)
    if layer == "bronze" and sub_layer == "l02":
        return _populate_cdc(template, object_key, hop_config, primary_keys, hop_defaults)
    if layer == "silver" and sub_layer == "l03":
        return _populate_processed(template, object_key, primary_keys, hop_defaults)
    if layer == "silver" and sub_layer == "l04":
        return _populate_skey(template, object_key, hop_config, primary_keys, hop_defaults)
    return template


# ── Generator ───────────────────────────────────────────────────────────────────

def generate_configs(bundle_or_target=None, config_root: Optional[str] = None):
    """
    Generate per-object config YAML files from objects.yml.

    Accepts either a Bundle instance (called from load_resources during bundle
    deploy) or a target name string (called standalone). Defaults to 'dev'.
    """
    target = "dev"
    if isinstance(bundle_or_target, str):
        target = bundle_or_target

    if config_root is None:
        config_root = str(BUNDLE_ROOT / "config")

    root = Path(config_root)
    hop_defaults = load_hop_defaults(target)
    source_systems = load_source_systems(config_root)
    templates = load_templates(str(root / "config_templates"))

    if not source_systems:
        print("No source systems found in objects.yml — nothing to generate.")
        return

    created, updated = [], []

    for source_name, source in source_systems.items():
        if not source:
            print(f"  WARNING: '{source_name}' has no configuration — skipping.")
            continue

        source_type = source.get("source_type", "")
        base_connection = source.get("connection", {})
        hop_layer_defaults = source.get("defaults", {})

        object_groups = [
            ("dimension", source.get("dimensions") or {}),
            ("fact",      source.get("facts")      or {}),
        ]

        for object_type, objects in object_groups:
            for object_key, object_value in objects.items():
                primary_keys, _overrides = parse_object(object_value)
                connection = resolve_connection(base_connection, object_key)

                for layer in ("bronze", "silver"):
                    layer_defaults = hop_layer_defaults.get(layer, {})
                    if not layer_defaults:
                        continue

                    for sub_layer, hop_config in layer_defaults.items():
                        if not hop_config:
                            continue
                        if hop_config.get("enabled") is False:
                            continue
                        if not hop_config.get("metadata_driven"):
                            continue

                        # scd_type for CDC and skey layers is derived from object classification
                        if sub_layer in ("l02", "l04"):
                            hop_config = {**hop_config, "scd_type": 1 if object_type == "fact" else 2}

                        folder_key = (layer, sub_layer)
                        template_name = TEMPLATE_MAP.get(folder_key)
                        hop_name = hop_defaults.get(folder_key, {}).get("name")
                        layer_folder = LAYER_FOLDERS.get(layer)

                        if not template_name or not hop_name or not layer_folder:
                            print(f"  WARNING: No mapping for ({layer}, {sub_layer}) — skipping.")
                            continue

                        folder_path = root / layer_folder / hop_name
                        file_path = folder_path / f"{hop_name}_{object_key}.yml"
                        folder_path.mkdir(parents=True, exist_ok=True)

                        content = _dispatch(
                            templates[template_name],
                            object_key, layer, sub_layer,
                            hop_config, source_type, connection, primary_keys,
                            hop_defaults,
                        )
                        content = _strip_template_header(content)

                        existed = file_path.exists()
                        file_path.write_text(content)
                        (updated if existed else created).append(str(file_path))

    total = len(created) + len(updated)
    print(f"Generated {total} file(s) — {len(created)} new, {len(updated)} updated.")
    for fp in created:
        print(f"  + {fp}")
    for fp in updated:
        print(f"  ~ {fp}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate per-object config files from objects.yml.")
    parser.add_argument("--target", default="dev", help="Databricks target (default: dev)")
    parser.add_argument(
        "--config-root",
        default=str(BUNDLE_ROOT / "config"),
        help="Config directory path (default: <bundle_root>/config)",
    )
    args = parser.parse_args()
    generate_configs(args.target, args.config_root)
