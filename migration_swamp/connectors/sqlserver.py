"""SQL Server connector. Pure helpers are unit-tested; the Connector class
is thin Spark/JDBC glue hardened at work against the approved pattern."""
from migration_swamp.connectors.base import (
    Asset, ConnectorError, ProbeResult,
)
from migration_swamp.crypto import Credentials
from migration_swamp.status import Status

_AUTH_MARKERS = ("login failed", "ora-01017", "authentication failed")
_NOT_FOUND_MARKERS = ("invalid object name", "ora-00942", "does not exist",
                      "not found")


def build_jdbc_url(dbhost: str) -> str:
    return f"jdbc:sqlserver://{dbhost};encrypt=true;trustServerCertificate=true"


def build_probe_query(schema: str, table: str) -> str:
    return f"SELECT * FROM {schema}.{table} WHERE 1=0"


def classify_jdbc_error(message: str) -> Status:
    lower = message.lower()
    if any(m in lower for m in _AUTH_MARKERS):
        return Status.AUTH_FAILED
    if any(m in lower for m in _NOT_FOUND_MARKERS):
        return Status.ASSET_NOT_FOUND
    return Status.DRIVER_ERROR


class SqlServerConnector:
    def __init__(self, spark):
        self._spark = spark

    def _reader(self, asset: Asset, creds: Credentials, query: str):
        return (self._spark.read.format("jdbc")
                .option("url", build_jdbc_url(asset.dbhost))
                .option("query", query)
                .option("user", creds.username)
                .option("password", creds.password))

    def probe(self, asset: Asset, creds: Credentials) -> ProbeResult:
        try:
            self._reader(asset, creds,
                         build_probe_query(asset.schema, asset.table)).load()
            return ProbeResult(ok=True)
        except Exception as exc:  # noqa: BLE001 - classified below
            return ProbeResult(ok=False, status=classify_jdbc_error(str(exc)),
                               message=str(exc))

    def read_to_staging(self, asset: Asset, creds: Credentials,
                        staging_view: str) -> int:
        try:
            df = self._reader(
                asset, creds,
                f"SELECT * FROM {asset.schema}.{asset.table}").load()
            df.createOrReplaceTempView(staging_view)
            return df.count()
        except Exception as exc:  # noqa: BLE001
            raise ConnectorError(classify_jdbc_error(str(exc)),
                                 str(exc)) from exc
