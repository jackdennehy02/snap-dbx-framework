import yaml
from pyspark.sql import functions as F


# ── Timestamp parsing ────────────────────────────────────────────────────────

# Tried in order — first non-null result wins.
# to_timestamp returns null on format mismatch (Databricks default, ANSI off).
TIMESTAMP_FORMATS = [
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSSX",   # ISO 8601 microseconds + tz
    "yyyy-MM-dd'T'HH:mm:ss.SSSX",      # ISO 8601 milliseconds + tz
    "yyyy-MM-dd'T'HH:mm:ssX",          # ISO 8601 + tz
    "yyyy-MM-dd'T'HH:mm:ss.SSS",       # ISO 8601 milliseconds, no tz
    "yyyy-MM-dd'T'HH:mm:ss",           # ISO 8601 no tz
    "yyyy-MM-dd HH:mm:ss.SSS",         # standard datetime + ms
    "yyyy-MM-dd HH:mm:ss",             # standard datetime
    "yyyy-MM-dd",                       # date only
    "MM/dd/yyyy HH:mm:ss",             # US datetime
    "MM/dd/yyyy",                       # US date
    "dd/MM/yyyy HH:mm:ss",             # European datetime
    "dd/MM/yyyy",                       # European date
    "dd-MM-yyyy HH:mm:ss",             # European dash datetime
    "dd-MM-yyyy",                       # European dash date
    "yyyyMMdd HHmmss",                  # compact datetime
    "yyyyMMdd",                         # compact date
]


def parse_timestamp_robust(col: F.Column) -> F.Column:
    """Parse a string column to timestamp across all known formats.

    Tries each format in TIMESTAMP_FORMATS via coalesce. Falls back to a
    direct cast as a last resort (handles epoch integers and any formats
    Spark can infer automatically). Returns null if nothing matches.
    """
    attempts = [F.to_timestamp(col, fmt) for fmt in TIMESTAMP_FORMATS]
    attempts.append(col.cast("timestamp"))
    return F.coalesce(*attempts)


def source_timestamp(df, col_name: str) -> F.Column:
    """Return a timestamp Column for col_name, handling any source dtype."""
    dtype = dict(df.dtypes).get(col_name, "string")
    if dtype in ("timestamp", "timestamp_ntz"):
        return F.col(col_name)
    if dtype == "date":
        return F.col(col_name).cast("timestamp")
    return parse_timestamp_robust(F.col(col_name))


# ── YAML helpers ─────────────────────────────────────────────────────────────

def load_yaml(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_objects(config_root: str, layer_id: str) -> list[str]:
    """Return object keys with the given layer enabled in objects.yml."""
    registry = load_yaml(f"{config_root}/objects.yml")
    result = []
    for source in registry.get("source_systems", {}).values():
        if not source:
            continue
        hop_config = source.get("defaults", {}).get(layer_id, {})
        if not hop_config or not hop_config.get("enabled", False):
            continue
        for objects in (source.get("dimensions") or {}, source.get("facts") or {}):
            result.extend(objects.keys())
    return result


def load_hop_config(config_root: str, location: str, hop_name: str, object_key: str) -> dict:
    """Load a per-object hop config from config/{location}/{hop_name}/{hop_name}_{object_key}.yml."""
    return load_yaml(f"{config_root}/{location}/{hop_name}/{hop_name}_{object_key}.yml")
