from migration_swamp.governance import (
    build_create_table, build_grant_select, build_set_tags,
)
from migration_swamp.naming import target_path
from migration_swamp.request import AcquisitionRequest

TP = target_path("sql_server", "prod_db", "data_table")
REQ = AcquisitionRequest("r1", "user@example.com", "sql_server", "prod_db",
                         "data_table", "sqlhost01", True, False)


def test_create_table_full_replace():
    sql = build_create_table(TP, "_swamp_staging")
    assert sql == ("CREATE OR REPLACE TABLE `sql_server`.`prod_db`.`data_table` "
                   "AS SELECT * FROM _swamp_staging")


def test_set_tags_includes_required_tags():
    sql = build_set_tags(TP, REQ, 42, "2026-08-14T12:00:00Z")
    assert sql.startswith(
        "ALTER TABLE `sql_server`.`prod_db`.`data_table` SET TAGS ("
    )
    assert "'source_system' = 'sql_server'" in sql
    assert "'acquisition_type' = 'copy'" in sql
    assert "'acquired_by' = 'user@example.com'" in sql
    assert "'acquired_at' = '2026-08-14T12:00:00Z'" in sql
    assert "'source_host' = 'sqlhost01'" in sql
    assert "'row_count' = '42'" in sql


def test_set_tags_escapes_quotes_and_omits_missing_host():
    req = AcquisitionRequest("r1", "o'brien@example.com", "oracle", "gl",
                             "bal", None, True, False)
    sql = build_set_tags(target_path("oracle", "gl", "bal"), req, None, "t0")
    assert "o''brien@example.com" in sql
    assert "source_host" not in sql
    assert "row_count" not in sql


def test_grant_select():
    sql = build_grant_select(TP, "user@example.com")
    assert sql == ("GRANT SELECT ON TABLE `sql_server`.`prod_db`.`data_table` "
                   "TO `user@example.com`")
