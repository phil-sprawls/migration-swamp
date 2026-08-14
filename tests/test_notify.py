from migration_swamp.naming import target_path
from migration_swamp.notify import LoggingNotifier, compose_failure, compose_success
from migration_swamp.request import AcquisitionRequest
from migration_swamp.status import Status

REQ = AcquisitionRequest("r1", "user@example.com", "sql_server", "prod_db",
                         "data_table", "sqlhost01", True, True)
TP = target_path("sql_server", "prod_db", "data_table")


def test_success_email_has_path_and_rows():
    subject, body = compose_success(REQ, TP, 42)
    assert "ready" in subject.lower()
    assert "sql_server.prod_db.data_table" in body
    assert "42" in body
    assert "serverless" in body.lower()  # how to query next


def test_failure_email_has_status_and_hint():
    subject, body = compose_failure(REQ, Status.AUTH_FAILED, "check password")
    assert "failed" in subject.lower()
    assert "AUTH_FAILED" in body and "check password" in body
    assert "r1" in body  # request id for support


def test_logging_notifier_records():
    n = LoggingNotifier()
    n.send("user@example.com", "s", "b")
    assert n.sent == [("user@example.com", "s", "b")]
