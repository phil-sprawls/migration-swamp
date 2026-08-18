"""SQL Server connector. Pure helpers are unit-tested; the Connector class
is thin Spark/JDBC glue hardened at work against the approved pattern."""
import socket

from migration_swamp.config import (
    DEFAULT_SQLSERVER_PORT, PREFLIGHT_TIMEOUT_SECONDS,
)
from migration_swamp.connectors.base import (
    Asset, ConnectorError, ProbeResult,
)
from migration_swamp.crypto import Credentials
from migration_swamp.status import Status

_AUTH_MARKERS = ("login failed", "ora-01017", "authentication failed")
_NOT_FOUND_MARKERS = ("invalid object name", "ora-00942", "does not exist",
                      "not found")
# A firewall drop surfaces as a driver-level socket failure, never as a login
# error: the packet dies before SQL Server ever sees a login attempt. Note
# "connection reset" is deliberately absent — a reset mid-query is a normal
# transport fault and should stay DRIVER_ERROR rather than send the user to
# the intake form.
_BLOCKED_MARKERS = ("the tcp/ip connection to the host", "socket timeout",
                    "connection timed out", "connect timed out",
                    "connection refused", "no route to host",
                    "network is unreachable", "sockettimeoutexception",
                    "socketexception")


def build_jdbc_url(dbhost: str) -> str:
    return f"jdbc:sqlserver://{dbhost};encrypt=true;trustServerCertificate=true"


def build_probe_query(schema: str, table: str) -> str:
    return f"SELECT * FROM {schema}.{table} WHERE 1=0"


def classify_jdbc_error(message: str) -> Status:
    """Shared with the Oracle connector. Deliberately has no firewall branch:
    the Oracle endpoint is already enabled tenant-wide, so Oracle users must
    not be routed to the SQL Server intake form."""
    lower = message.lower()
    if any(m in lower for m in _AUTH_MARKERS):
        return Status.AUTH_FAILED
    if any(m in lower for m in _NOT_FOUND_MARKERS):
        return Status.ASSET_NOT_FOUND
    return Status.DRIVER_ERROR


def classify_sqlserver_error(message: str) -> Status:
    """SQL Server only: firewall blocks first, then the shared rules."""
    lower = message.lower()
    if any(m in lower for m in _BLOCKED_MARKERS):
        return Status.NETWORK_BLOCKED
    return classify_jdbc_error(message)


def split_host_port(dbhost: str) -> tuple[str, int | None]:
    """Split a dbhost widget value into (host, port) for the preflight check.

    Accepts "host", "host:port", "host\\instance" and "host\\instance:port".
    Port is None when it cannot be known ahead of time: a named instance
    without an explicit port is resolved dynamically by SQL Browser, so
    probing 1433 would report a block that isn't real."""
    hostpart, sep, portpart = dbhost.strip().partition(":")
    machine, instance, _ = hostpart.strip().partition("\\")
    machine = machine.strip()
    if sep:
        try:
            return machine, int(portpart.strip())
        except ValueError:
            return machine, None
    return machine, None if instance else DEFAULT_SQLSERVER_PORT


def tcp_preflight(dbhost: str, timeout: float = PREFLIGHT_TIMEOUT_SECONDS,
                  connect=None) -> bool | None:
    """Can this cluster open a TCP socket to the SQL Server port?

    True = reachable, False = blocked, None = undetermined (no port to test).
    `connect` is injectable so the logic is unit-testable without a network.
    A dropped packet would otherwise hang the JDBC login for 30s or more;
    this answers in `timeout` seconds."""
    host, port = split_host_port(dbhost)
    if not host or port is None:
        return None
    opener = connect or socket.create_connection
    try:
        opener((host, port), timeout).close()
        return True
    except OSError:
        return False


def preflight_failure(dbhost: str,
                      timeout: float = PREFLIGHT_TIMEOUT_SECONDS,
                      connect=None) -> ProbeResult | None:
    """A NETWORK_BLOCKED ProbeResult when the host is unreachable, else None
    to carry on with the real connection attempt."""
    if tcp_preflight(dbhost, timeout, connect) is not False:
        return None
    host, port = split_host_port(dbhost)
    return ProbeResult(
        ok=False, status=Status.NETWORK_BLOCKED,
        message=(f"Databricks could not open a network connection to "
                 f"{host}:{port} within {timeout:g}s."))


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
        # Check reachability before spending a login timeout on a host the
        # firewall is dropping; classify_sqlserver_error is the backstop for
        # blocks the socket check cannot see (named instances, proxies).
        blocked = preflight_failure(asset.dbhost or "")
        if blocked is not None:
            return blocked
        try:
            self._reader(asset, creds,
                         build_probe_query(asset.schema, asset.table)).load()
            return ProbeResult(ok=True)
        except Exception as exc:  # noqa: BLE001 - classified below
            return ProbeResult(ok=False,
                               status=classify_sqlserver_error(str(exc)),
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
            raise ConnectorError(classify_sqlserver_error(str(exc)),
                                 str(exc)) from exc
