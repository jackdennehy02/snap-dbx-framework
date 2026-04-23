from databricks.bundles.core import Bundle, Resources

from resources.config_generator import generate_configs


def load_resources(bundle: Bundle) -> Resources:
    """
    Called by the Databricks CLI during bundle deployment.
    Generates per-object YAML config files from objects.yml.
    Pipeline resources remain defined in databricks.yml.
    """
    generate_configs(bundle)
    return Resources()
