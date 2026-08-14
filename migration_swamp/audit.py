from migration_swamp.config import AUDIT_TABLE
from migration_swamp.naming import TargetPath
from migration_swamp.request import AcquisitionRequest
from migration_swamp.status import Status

COLUMNS = [
    ("request_id", "STRING"), ("requester", "STRING"),
    ("source_system", "STRING"), ("source_schema", "STRING"),
    ("source_table", "STRING"), ("dbhost", "STRING"),
    ("gain_access", "BOOLEAN"), ("refresh", "BOOLEAN"),
    ("target_table", "STRING"), ("status", "STRING"),
    ("row_count", "BIGINT"), ("started_at", "STRING"),
    ("finished_at", "STRING"), ("hint", "STRING"),
]


def build_ensure_table(audit_table: str = AUDIT_TABLE) -> str:
    cols = ", ".join(f"{name} {typ}" for name, typ in COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {audit_table} ({cols})"


def build_row(req: AcquisitionRequest, target: TargetPath | None,
              status: Status, row_count: int | None, started_at: str,
              finished_at: str, hint: str) -> dict:
    return {
        "request_id": req.request_id,
        "requester": req.requester,
        "source_system": req.source_system,
        "source_schema": req.schema,
        "source_table": req.table,
        "dbhost": req.dbhost,
        "gain_access": req.gain_access,
        "refresh": req.refresh,
        "target_table": target.display if target else None,
        "status": status.value,
        "row_count": row_count,
        "started_at": started_at,
        "finished_at": finished_at,
        "hint": hint,
    }


def _literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def build_insert(row: dict, audit_table: str = AUDIT_TABLE) -> str:
    names = ", ".join(name for name, _ in COLUMNS)
    values = ", ".join(_literal(row[name]) for name, _ in COLUMNS)
    return f"INSERT INTO {audit_table} ({names}) VALUES ({values})"
