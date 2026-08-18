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


def test_classify_jdbc_error_has_no_firewall_branch():
    """Oracle shares this classifier and its endpoint is already enabled,
    so a socket failure here must not route Oracle users to the intake."""
    assert sqlserver.classify_jdbc_error(
        "The TCP/IP connection to the host sqlhost01 has failed"
    ) is Status.DRIVER_ERROR


def test_classify_sqlserver_error_detects_firewall_block():
    for message in (
        "The TCP/IP connection to the host sqlhost01, port 1433 has failed",
        "java.net.SocketTimeoutException: connect timed out",
        "Connection refused: no further information",
        "java.net.NoRouteToHostException: No route to host",
    ):
        assert sqlserver.classify_sqlserver_error(
            message) is Status.NETWORK_BLOCKED


def test_classify_sqlserver_error_still_defers_to_shared_rules():
    assert sqlserver.classify_sqlserver_error(
        "Login failed for user 'x'") is Status.AUTH_FAILED
    assert sqlserver.classify_sqlserver_error(
        "Invalid object name 'prod_db.data_table'") is Status.ASSET_NOT_FOUND
    # A mid-query reset is a transport fault, not a closed firewall path.
    assert sqlserver.classify_sqlserver_error(
        "connection reset by peer") is Status.DRIVER_ERROR


def test_split_host_port():
    assert sqlserver.split_host_port("sqlhost01") == ("sqlhost01", 1433)
    assert sqlserver.split_host_port("sqlhost01:1450") == ("sqlhost01", 1450)
    assert sqlserver.split_host_port("  sqlhost01  ") == ("sqlhost01", 1433)
    # Named instance: port is resolved dynamically, so it is not knowable.
    assert sqlserver.split_host_port("sqlhost01\\PROD") == ("sqlhost01", None)
    assert sqlserver.split_host_port("sqlhost01\\PROD:1450") == (
        "sqlhost01", 1450)
    assert sqlserver.split_host_port("sqlhost01:notaport") == (
        "sqlhost01", None)


def _refuse(address, timeout):
    raise OSError("connection refused")


class _FakeSocket:
    def close(self):
        pass


def _accept(address, timeout):
    return _FakeSocket()


def test_tcp_preflight_tristate():
    assert sqlserver.tcp_preflight("sqlhost01", connect=_accept) is True
    assert sqlserver.tcp_preflight("sqlhost01", connect=_refuse) is False
    # Undetermined: no port to test, so let the driver try.
    assert sqlserver.tcp_preflight("sqlhost01\\PROD", connect=_refuse) is None
    assert sqlserver.tcp_preflight("", connect=_refuse) is None


def test_preflight_failure_builds_blocked_probe_result():
    result = sqlserver.preflight_failure("sqlhost01", connect=_refuse)
    assert result is not None
    assert result.ok is False
    assert result.status is Status.NETWORK_BLOCKED
    assert "sqlhost01:1433" in result.message


def test_preflight_failure_returns_none_when_reachable_or_unknown():
    assert sqlserver.preflight_failure("sqlhost01", connect=_accept) is None
    assert sqlserver.preflight_failure(
        "sqlhost01\\PROD", connect=_refuse) is None
