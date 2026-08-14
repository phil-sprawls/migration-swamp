from migration_swamp.audit import build_ensure_table, build_insert, build_row
from migration_swamp.naming import target_path
from migration_swamp.request import AcquisitionRequest
from migration_swamp.status import Status

REQ = AcquisitionRequest("r1", "user@example.com", "sql_server", "prod_db",
                         "data_table", "sqlhost01", True, False)
TP = target_path("sql_server", "prod_db", "data_table")


def test_ensure_table_is_create_if_not_exists():
    sql = build_ensure_table("swamp_meta.ops.acquisition_log")
    assert sql.startswith(
        "CREATE TABLE IF NOT EXISTS swamp_meta.ops.acquisition_log"
    )
    for col in ("request_id", "status", "hint", "row_count"):
        assert col in sql


def test_build_row_shape():
    row = build_row(REQ, TP, Status.SUCCEEDED, 42, "t0", "t1", "")
    assert row["request_id"] == "r1"
    assert row["target_table"] == "sql_server.prod_db.data_table"
    assert row["status"] == "SUCCEEDED"
    assert row["row_count"] == 42
    assert row["gain_access"] is True and row["refresh"] is False


def test_build_row_no_target_yet():
    row = build_row(REQ, None, Status.POLICY_REJECTED, None, "t0", "t1",
                    "fix your request")
    assert row["target_table"] is None
    assert row["hint"] == "fix your request"


def test_build_insert_escapes_and_handles_nulls():
    row = build_row(REQ, None, Status.AUTH_FAILED, None, "t0", "t1",
                    "check o'brien account")
    sql = build_insert(row, "a.b.c")
    assert sql.startswith("INSERT INTO a.b.c")
    assert "o''brien" in sql
    assert "NULL" in sql  # target_table and row_count
