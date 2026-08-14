from typing import Protocol

from migration_swamp.naming import TargetPath
from migration_swamp.request import AcquisitionRequest
from migration_swamp.status import Status


class Notifier(Protocol):
    def send(self, to: str, subject: str, body: str) -> None: ...


class LoggingNotifier:
    """Laptop/default notifier. Work hardening swaps in the approved
    company email pattern behind the same send() signature."""

    def __init__(self):
        self.sent: list[tuple[str, str, str]] = []

    def send(self, to: str, subject: str, body: str) -> None:
        self.sent.append((to, subject, body))
        print(f"[notify] to={to} subject={subject}\n{body}")


def compose_success(req: AcquisitionRequest, target: TargetPath,
                    row_count: int | None) -> tuple[str, str]:
    subject = f"Your data is ready: {target.display}"
    rows = f"{row_count} rows copied." if row_count is not None else \
        "Access granted to the existing copy (no new copy needed)."
    body = (
        f"Request {req.request_id} succeeded.\n\n"
        f"Table: {target.display}\n{rows}\n\n"
        f"Query it from any serverless SQL warehouse:\n"
        f"  SELECT * FROM {target.display} LIMIT 100\n\n"
        f"To refresh this data later, re-run the Acquire notebook with "
        f"'Refresh data' selected."
    )
    return subject, body


def compose_failure(req: AcquisitionRequest, status: Status,
                    hint: str) -> tuple[str, str]:
    subject = f"Data acquisition failed: {req.schema}.{req.table}"
    body = (
        f"Request {req.request_id} failed with status {status.value}.\n\n"
        f"What to do: {hint}\n\n"
        f"Source: {req.source_system} {req.schema}.{req.table}\n"
        f"If you need help, contact the platform team and include request "
        f"id {req.request_id}."
    )
    return subject, body
