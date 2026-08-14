"""Oracle connector. Endpoint is fixed in config; same JDBC shape as
SQL Server with an Oracle URL."""
from migration_swamp.connectors.base import (
    Asset, ConnectorError, ProbeResult,
)
from migration_swamp.connectors.sqlserver import (
    build_probe_query, classify_jdbc_error,
)
from migration_swamp.crypto import Credentials


def build_jdbc_url(endpoint: str) -> str:
    return f"jdbc:oracle:thin:@//{endpoint}"


class OracleConnector:
    def __init__(self, spark, endpoint: str):
        self._spark = spark
        self._endpoint = endpoint

    def _reader(self, creds: Credentials, query: str):
        return (self._spark.read.format("jdbc")
                .option("url", build_jdbc_url(self._endpoint))
                .option("query", query)
                .option("user", creds.username)
                .option("password", creds.password))

    def probe(self, asset: Asset, creds: Credentials) -> ProbeResult:
        try:
            self._reader(creds,
                         build_probe_query(asset.schema, asset.table)).load()
            return ProbeResult(ok=True)
        except Exception as exc:  # noqa: BLE001
            return ProbeResult(ok=False, status=classify_jdbc_error(str(exc)),
                               message=str(exc))

    def read_to_staging(self, asset: Asset, creds: Credentials,
                        staging_view: str) -> int:
        try:
            df = self._reader(
                creds, f"SELECT * FROM {asset.schema}.{asset.table}").load()
            df.createOrReplaceTempView(staging_view)
            return df.count()
        except Exception as exc:  # noqa: BLE001
            raise ConnectorError(classify_jdbc_error(str(exc)),
                                 str(exc)) from exc
