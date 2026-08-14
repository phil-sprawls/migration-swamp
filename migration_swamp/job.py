from collections.abc import Callable
from dataclasses import dataclass

from migration_swamp import audit, governance, messages
from migration_swamp.config import AUDIT_TABLE
from migration_swamp.connectors.base import Asset, Connector, ConnectorError
from migration_swamp.crypto import CryptoError, decrypt_credentials
from migration_swamp.executor import SqlExecutor
from migration_swamp.naming import TargetPath, target_path
from migration_swamp.notify import Notifier, compose_failure, compose_success
from migration_swamp.request import (
    AcquisitionRequest, RequestError, from_params, validate,
)
from migration_swamp.scrub import scrub
from migration_swamp.status import Status

STAGING_VIEW = "_swamp_staging"


@dataclass
class JobDeps:
    connector_factory: Callable[[AcquisitionRequest], Connector]
    executor: SqlExecutor
    notifier: Notifier
    private_key_pem: str
    now: Callable[[], str]
    audit_table: str = AUDIT_TABLE


@dataclass(frozen=True)
class JobResult:
    status: Status
    target_display: str | None
    row_count: int | None


def run(params: dict[str, str], envelope: str, deps: JobDeps) -> JobResult:
    started_at = deps.now()
    secrets: list[str] = []
    target: TargetPath | None = None
    row_count: int | None = None
    audited = False
    notified = False

    def finish(status: Status, hint: str = "") -> JobResult:
        nonlocal audited, notified
        finished_at = deps.now()
        hint = hint or messages.HINTS.get(status, "")
        req_for_audit = from_params(params)  # re-create for audit
        row = audit.build_row(req_for_audit, target, status, row_count,
                              started_at, finished_at, scrub(hint, secrets))
        deps.executor.execute(audit.build_ensure_table(deps.audit_table))
        if not audited:
            deps.executor.execute(audit.build_insert(row, deps.audit_table))
            audited = True
        if status is Status.SUCCEEDED:
            subject, body = compose_success(req_for_audit, target, row_count)
        else:
            subject, body = compose_failure(req_for_audit, status,
                                           scrub(hint, secrets))
        if not notified:
            deps.notifier.send(req_for_audit.requester, scrub(subject, secrets),
                               scrub(body, secrets))
            notified = True
        return JobResult(status, target.display if target else None, row_count)

    try:
        try:
            req = from_params(params)
            validate(req)
        except RequestError as exc:
            return finish(Status.POLICY_REJECTED, str(exc))

        target = target_path(req.source_system, req.schema, req.table)

        try:
            creds = decrypt_credentials(envelope, deps.private_key_pem)
        except CryptoError:
            target = None
            return finish(
                Status.POLICY_REJECTED,
                "The credential payload could not be read. Re-run the "
                "Acquire notebook to submit a fresh request.")
        secrets = [creds.username, creds.password]

        connector = deps.connector_factory(req)
        asset = Asset(req.source_system, req.schema, req.table, req.dbhost)

        probe = connector.probe(asset, creds)
        if not probe.ok:
            return finish(probe.status or Status.DRIVER_ERROR,
                          messages.HINTS.get(
                              probe.status or Status.DRIVER_ERROR, ""))

        exists = deps.executor.table_exists(target)
        need_pull = (not exists) or req.refresh

        if need_pull:
            row_count = connector.read_to_staging(asset, creds, STAGING_VIEW)
            deps.executor.execute(
                governance.build_create_table(target, STAGING_VIEW))
            deps.executor.execute(
                governance.build_set_tags(target, req, row_count, deps.now()))

        deps.executor.execute(
            governance.build_grant_select(target, req.requester))
        return finish(Status.SUCCEEDED)

    except ConnectorError as exc:
        try:
            return finish(exc.status, scrub(str(exc), secrets))
        except Exception:  # noqa: BLE001
            # If finish() itself fails, swallow and return with DRIVER_ERROR
            return JobResult(Status.DRIVER_ERROR,
                           target.display if target else None, row_count)
    except Exception as exc:  # noqa: BLE001 - job must always audit+notify
        try:
            return finish(Status.DRIVER_ERROR,
                          messages.HINTS[Status.DRIVER_ERROR]
                          + " Detail: " + scrub(str(exc), secrets))
        except Exception:  # noqa: BLE001
            # If finish() itself fails, swallow and return with DRIVER_ERROR
            return JobResult(Status.DRIVER_ERROR,
                           target.display if target else None, row_count)
