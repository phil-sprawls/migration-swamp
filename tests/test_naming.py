from migration_swamp.naming import TargetPath, sanitize_identifier, target_path


def test_sanitize_lowercases_and_replaces():
    assert sanitize_identifier("Prod-DB") == "prod_db"
    assert sanitize_identifier("Data.Table$2") == "data_table_2"


def test_sanitize_leading_digit_prefixed():
    assert sanitize_identifier("2024_claims") == "t_2024_claims"


def test_sanitize_collapses_repeats():
    assert sanitize_identifier("a--b__c") == "a_b_c"


def test_target_path_matches_spec_example():
    tp = target_path("sql_server", "prod_db", "data_table")
    assert tp == TargetPath("sql_server", "prod_db", "data_table")
    assert tp.fqn == "`sql_server`.`prod_db`.`data_table`"
    assert tp.display == "sql_server.prod_db.data_table"


def test_target_path_is_deterministic_and_sanitized():
    a = target_path("oracle", "GL.Main", "Balances")
    b = target_path("oracle", "gl.main", "balances")
    assert a == b == TargetPath("oracle", "gl_main", "balances")
