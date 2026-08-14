from typing import Protocol

from migration_swamp.naming import TargetPath


class SqlExecutor(Protocol):
    def execute(self, sql: str) -> None: ...

    def table_exists(self, target: TargetPath) -> bool: ...


class SparkExecutor:
    """Real executor for the Databricks job. Lazy: no pyspark at import."""

    def __init__(self, spark):
        self._spark = spark

    def execute(self, sql: str) -> None:
        self._spark.sql(sql)

    def table_exists(self, target: TargetPath) -> bool:
        return self._spark.catalog.tableExists(target.display)
