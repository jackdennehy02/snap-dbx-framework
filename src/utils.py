import yaml


def load_yaml(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


def load_objects(config_root: str, layer: str, sub_layer: str) -> list[str]:
    """Return object keys with the given layer/sub_layer enabled in objects.yml."""
    registry = load_yaml(f"{config_root}/objects.yml")
    result = []
    for source in registry.get("source_systems", {}).values():
        if not source:
            continue
        hop_config = source.get("defaults", {}).get(layer, {}).get(sub_layer, {})
        if not hop_config or not hop_config.get("enabled", False):
            continue
        for objects in (source.get("dimensions") or {}, source.get("facts") or {}):
            result.extend(objects.keys())
    return result


def load_hop_config(config_root: str, layer_path: str, object_key: str) -> dict:
    """Load a per-object hop config from config/{layer_path}/{hop_name}_{object_key}.yml."""
    hop_name = layer_path.split("/")[-1]
    return load_yaml(f"{config_root}/{layer_path}/{hop_name}_{object_key}.yml")
