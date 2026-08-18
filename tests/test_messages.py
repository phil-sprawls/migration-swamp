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


def test_network_blocked_hint_points_at_the_intake():
    hint = messages.HINTS[Status.NETWORK_BLOCKED]
    assert "go/udapintake" in hint
    # Must tell the user this is not their credentials or their table name.
    assert "firewall" in hint.lower()


def test_probe_failed_appends_the_hint_for_a_status():
    text = messages.probe_failed(
        "Databricks could not open a network connection to sqlhost01:1433 "
        "within 5s.", Status.NETWORK_BLOCKED)
    assert "sqlhost01:1433" in text
    assert "go/udapintake" in text
    # Re-running cannot help until the path is opened.
    assert "re-run this cell" not in text


def test_probe_failed_without_status_is_unchanged():
    text = messages.probe_failed("denied")
    assert "denied" in text
    assert "re-run this cell" in text
    assert "go/udapintake" not in text


def test_every_failure_status_has_a_hint():
    for status in Status:
        if status is not Status.SUCCEEDED:
            assert messages.HINTS[status]
