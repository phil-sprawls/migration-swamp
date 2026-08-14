from migration_swamp.connectors.base import Asset, ConnectorError, ProbeResult
from migration_swamp.crypto import Credentials
from migration_swamp.naming import TargetPath


class FakeConnector:
    def __init__(self, probe_result: ProbeResult = ProbeResult(ok=True),
                 rows: int = 42, read_error: ConnectorError | None = None):
        self.probe_result = probe_result
        self.rows = rows
        self.read_error = read_error
        self.probed_with: list[tuple[Asset, Credentials]] = []
        self.read_calls: list[tuple[Asset, str]] = []

    def probe(self, asset: Asset, creds: Credentials) -> ProbeResult:
        self.probed_with.append((asset, creds))
        return self.probe_result

    def read_to_staging(self, asset: Asset, creds: Credentials,
                        staging_view: str) -> int:
        self.read_calls.append((asset, staging_view))
        if self.read_error:
            raise self.read_error
        return self.rows


class FakeExecutor:
    def __init__(self, existing_tables: set[str] | None = None):
        self.existing = existing_tables or set()
        self.executed: list[str] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def table_exists(self, target: TargetPath) -> bool:
        return target.display in self.existing
