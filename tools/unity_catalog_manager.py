from pyspark.sql.utils import AnalysisException
import logging

logger = logging.getLogger(__name__)

class UnityManager:
    CONTAINER_TYPES = {"CATALOG", "SCHEMA", "VOLUME"}
    DATA_TYPES = {"TABLE", "VIEW", "MATERIALIZED VIEW"}
    ALL_TYPES = CONTAINER_TYPES | DATA_TYPES

    def __init__(self, spark, catalog=None, schema=None):
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    def _validate_type(self, asset_type, allowed=None):
        asset_type = asset_type.upper()
        valid = allowed or self.ALL_TYPES
        if asset_type not in valid:
            raise ValueError(f"Unsupported asset type: {asset_type}. Must be one of {valid}")
        return asset_type

    def _full_name(self, asset_type, asset_name):
        if "." in asset_name:
            return asset_name
        if asset_type == "CATALOG":
            return asset_name
        elif asset_type == "SCHEMA":
            if not self.catalog:
                raise ValueError("Catalog must be set to work with schemas")
            return f"{self.catalog}.{asset_name}"
        else:
            if not self.catalog or not self.schema:
                raise ValueError("Catalog and schema must be set to work with tables/views/volumes")
            return f"{self.catalog}.{self.schema}.{asset_name}"

    def _run(self, sql):
        logger.info(f"Executing: {sql}")
        return self.spark.sql(sql)

    def asset_exists(self, asset_type, asset_name):
        asset_type = self._validate_type(asset_type)
        full = self._full_name(asset_type, asset_name)
        try:
            self._run(f"DESCRIBE {asset_type} {full}")
            return True
        except AnalysisException:
            return False

    # --- Container operations ---

    def create_container(self, asset_type, asset_name, location=None):
        asset_type = self._validate_type(asset_type, self.CONTAINER_TYPES)
        full = self._full_name(asset_type, asset_name)
        sql = f"CREATE {asset_type} IF NOT EXISTS {full}"
        if location:
            sql += f" MANAGED LOCATION '{location}'"
        self._run(sql)

    def drop_container(self, asset_type, asset_name, cascade=False):
        asset_type = self._validate_type(asset_type, self.CONTAINER_TYPES)
        full = self._full_name(asset_type, asset_name)
        sql = f"DROP {asset_type} IF EXISTS {full}"
        if cascade:
            sql += " CASCADE"
        self._run(sql)

    # --- Data asset operations ---

    def create_table(self, table_name, schema_definition=None, location=None, file_format=None):
        full = self._full_name("TABLE", table_name)
        sql = f"CREATE TABLE IF NOT EXISTS {full}"
        if schema_definition:
            sql += f" ({schema_definition})"
        if file_format:
            sql += f" USING {file_format}"
        if location:
            sql += f" LOCATION '{location}'"
        self._run(sql)

    def create_view(self, view_name, query):
        full = self._full_name("VIEW", view_name)
        self._run(f"CREATE VIEW IF NOT EXISTS {full} AS {query}")

    def create_materialized_view(self, view_name, query):
        full = self._full_name("MATERIALIZED VIEW", view_name)
        self._run(f"CREATE MATERIALIZED VIEW IF NOT EXISTS {full} AS {query}")

    



