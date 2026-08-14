from migration_swamp.connectors import oracle, sqlserver
from migration_swamp.status import Status


def test_sqlserver_jdbc_url():
    url = sqlserver.build_jdbc_url("sqlhost01")
    assert url.startswith("jdbc:sqlserver://sqlhost01")
    assert "encrypt=true" in url


def test_oracle_jdbc_url():
    url = oracle.build_jdbc_url("oracle.example.internal:1521/ORCL")
    assert url == "jdbc:oracle:thin:@//oracle.example.internal:1521/ORCL"


def test_probe_query_is_zero_row():
    q = sqlserver.build_probe_query("prod_db", "data_table")
    assert q == "SELECT * FROM prod_db.data_table WHERE 1=0"
    assert oracle.build_probe_query("gl", "bal") == \
        "SELECT * FROM gl.bal WHERE 1=0"


def test_classify_jdbc_error():
    assert sqlserver.classify_jdbc_error(
        "Login failed for user 'x'") is Status.AUTH_FAILED
    assert sqlserver.classify_jdbc_error(
        "ORA-01017: invalid username/password") is Status.AUTH_FAILED
    assert sqlserver.classify_jdbc_error(
        "Invalid object name 'prod_db.data_table'") is Status.ASSET_NOT_FOUND
    assert sqlserver.classify_jdbc_error(
        "ORA-00942: table or view does not exist") is Status.ASSET_NOT_FOUND
    assert sqlserver.classify_jdbc_error(
        "connection reset by peer") is Status.DRIVER_ERROR
