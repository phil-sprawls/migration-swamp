from dataclasses import dataclass
from typing import Protocol

from migration_swamp.crypto import Credentials
from migration_swamp.status import Status


@dataclass(frozen=True)
class Asset:
    source_system: str
    schema: str
    table: str
    dbhost: str | None = None


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    status: Status | None = None
    message: str = ""


class ConnectorError(Exception):
    def __init__(self, status: Status, message: str):
        super().__init__(message)
        self.status = status


class Connector(Protocol):
    def probe(self, asset: Asset, creds: Credentials) -> ProbeResult:
        """Cheap table-level entitlement check (SELECT ... WHERE 1=0)."""

    def read_to_staging(self, asset: Asset, creds: Credentials,
                        staging_view: str) -> int:
        """Full-read the asset into a temp view; return row count.
        Raises ConnectorError on failure."""
