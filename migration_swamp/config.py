"""Deployment configuration. Values below are laptop defaults; the work
hardening pass replaces endpoints, AUDIT_TABLE, and JOB_NAME with
tenant-approved values."""
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SourceConfig:
    name: str
    requires_dbhost: bool   # True: user must supply dbhost (SQL Server)
    endpoint: str | None    # fixed endpoint for oracle/sas; None if per-request
    max_rows: int | None    # volume guardrail (SAS); None = no limit


SOURCES: dict[str, SourceConfig] = {
    "sql_server": SourceConfig("sql_server", True, None, None),
    "oracle": SourceConfig("oracle", False, "oracle.example.internal:1521/ORCL", None),
    "sas": SourceConfig("sas", False, "sas.example.internal", 5_000_000),
}

AUDIT_TABLE = "swamp_meta.ops.acquisition_log"
JOB_NAME = "migration-swamp-acquire"


def load_public_key() -> str:
    env = os.environ.get("SWAMP_PUBLIC_KEY_PEM")
    if env:
        return env
    dev_key = Path(__file__).resolve().parent.parent / "keys" / "dev_public.pem"
    return dev_key.read_text()
