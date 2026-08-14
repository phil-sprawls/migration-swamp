from migration_swamp import messages
from migration_swamp.status import Status


def test_copy_started_exact_wording():
    assert messages.COPY_STARTED == (
        "Data copy started. You may close this notebook. "
        "You will receive an email when the data is ready."
    )


def test_flow_messages_mention_asset():
    assert "sql_server.prod_db.data_table" in messages.probing(
        "sql_server.prod_db.data_table")
    assert "sql_server.prod_db.data_table" in messages.probe_ok(
        "sql_server.prod_db.data_table")
    assert "denied" in messages.probe_failed("denied")


def test_every_failure_status_has_a_hint():
    for status in Status:
        if status is not Status.SUCCEEDED:
            assert messages.HINTS[status]
