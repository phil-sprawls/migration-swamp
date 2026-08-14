from migration_swamp.connectors.base import ConnectorError, ProbeResult
from migration_swamp.crypto import Credentials, encrypt_credentials, generate_keypair
from migration_swamp.job import JobDeps, run
from migration_swamp.notify import LoggingNotifier
from migration_swamp.request import AcquisitionRequest, canonical_aad, to_params
from migration_swamp.status import Status
from tests.fakes import FakeConnector, FakeExecutor

PRIV, PUB = generate_keypair()
CREDS = Credentials("phil", "hunter2")
TARGET = "sql_server.prod_db.data_table"


def _base_kwargs(**kw):
    base = dict(request_id="r1", requester="user@example.com",
                source_system="sql_server", schema="prod_db",
                table="data_table", dbhost="sqlhost01",
                gain_access=True, refresh=False)
    base.update(kw)
    return base


def make_params(**kw):
    return to_params(AcquisitionRequest(**_base_kwargs(**kw)))


def make_deps(connector=None, executor=None):
    connector = connector or FakeConnector()
    executor = executor if executor is not None else FakeExecutor()
    notifier = LoggingNotifier()
    deps = JobDeps(connector_factory=lambda req: connector,
                   executor=executor, notifier=notifier,
                   private_key_pem=PRIV, now=lambda: "2026-08-14T12:00:00Z")
    return deps, connector, executor, notifier


def envelope(**kw):
    req = AcquisitionRequest(**_base_kwargs(**kw))
    return encrypt_credentials(CREDS, PUB, canonical_aad(req))


def audit_inserts(executor):
    return [s for s in executor.executed if s.startswith("INSERT INTO")]


def test_copy_missing_pulls_even_if_access_only():
    deps, conn, ex, notif = make_deps()
    result = run(make_params(gain_access=True, refresh=False), envelope(), deps)
    assert result.status is Status.SUCCEEDED
    assert result.target_display == TARGET and result.row_count == 42
    assert conn.read_calls  # pull happened
    assert any(s.startswith("CREATE OR REPLACE TABLE") for s in ex.executed)
    assert any("SET TAGS" in s for s in ex.executed)
    assert any(s.startswith("GRANT SELECT") for s in ex.executed)
    assert len(audit_inserts(ex)) == 1
    assert notif.sent and "ready" in notif.sent[0][1]


def test_exists_access_only_grants_without_pull():
    deps, conn, ex, notif = make_deps(executor=FakeExecutor({TARGET}))
    result = run(make_params(), envelope(), deps)
    assert result.status is Status.SUCCEEDED and result.row_count is None
    assert not conn.read_calls
    assert not any("CREATE OR REPLACE" in s for s in ex.executed)
    assert any(s.startswith("GRANT SELECT") for s in ex.executed)
    assert len(audit_inserts(ex)) == 1
    assert "no new copy needed" in notif.sent[0][2]


def test_exists_refresh_pulls_again():
    deps, conn, ex, _ = make_deps(executor=FakeExecutor({TARGET}))
    result = run(make_params(refresh=True), envelope(refresh=True), deps)
    assert result.status is Status.SUCCEEDED and result.row_count == 42
    assert conn.read_calls
    assert any("CREATE OR REPLACE" in s for s in ex.executed)


def test_invalid_params_policy_rejected():
    deps, conn, ex, notif = make_deps()
    result = run(make_params(dbhost=""), envelope(), deps)
    assert result.status is Status.POLICY_REJECTED
    assert not conn.probed_with and not conn.read_calls
    assert not any("CREATE OR REPLACE" in s or "GRANT" in s
                   for s in ex.executed)
    assert len(audit_inserts(ex)) == 1
    assert "failed" in notif.sent[0][1].lower()


def test_bad_envelope_policy_rejected():
    deps, _, ex, _ = make_deps()
    result = run(make_params(), "not-a-real-envelope", deps)
    assert result.status is Status.POLICY_REJECTED
    assert len(audit_inserts(ex)) == 1


def test_swapped_params_rejected():
    """Envelope bound to request A's params; run() called with request B's
    params (different table) must fail decryption, not silently reuse A's
    credentials for B's target."""
    deps, conn, ex, _ = make_deps()
    request_a_envelope = envelope(table="data_table")
    result = run(make_params(table="other_table"), request_a_envelope, deps)
    assert result.status is Status.POLICY_REJECTED
    assert not conn.probed_with and not conn.read_calls
    assert len(audit_inserts(ex)) == 1
    assert not any(s.startswith("CREATE OR REPLACE") for s in ex.executed)


def test_probe_failure_maps_connector_status():
    conn = FakeConnector(probe_result=ProbeResult(
        ok=False, status=Status.AUTH_FAILED, message="login failed"))
    deps, _, ex, notif = make_deps(connector=conn)
    result = run(make_params(), envelope(), deps)
    assert result.status is Status.AUTH_FAILED
    assert not conn.read_calls
    assert len(audit_inserts(ex)) == 1
    assert "AUTH_FAILED" in notif.sent[0][2]


def test_read_connector_error_maps_status():
    conn = FakeConnector(read_error=ConnectorError(
        Status.VOLUME_EXCEEDED, "too big"))
    deps, _, ex, _ = make_deps(connector=conn)
    result = run(make_params(), envelope(), deps)
    assert result.status is Status.VOLUME_EXCEEDED
    assert len(audit_inserts(ex)) == 1


def test_unexpected_error_is_driver_error_and_scrubbed():
    class ExplodingExecutor(FakeExecutor):
        def execute(self, sql):
            if sql.startswith("CREATE OR REPLACE"):
                raise RuntimeError(f"boom for phil/hunter2")
            super().execute(sql)

    deps, _, ex, notif = make_deps(executor=ExplodingExecutor())
    result = run(make_params(), envelope(), deps)
    assert result.status is Status.DRIVER_ERROR
    all_output = " ".join(ex.executed) + " ".join(
        subject + body for _, subject, body in notif.sent)
    assert "hunter2" not in all_output
    assert len(audit_inserts(ex)) == 1


def test_probe_uses_decrypted_user_creds():
    deps, conn, _, _ = make_deps()
    run(make_params(), envelope(), deps)
    assert conn.probed_with[0][1] == CREDS


def test_malformed_params_policy_rejected():
    """Missing params key should result in POLICY_REJECTED with one audit row."""
    params = make_params()
    del params["table"]  # Remove required field
    deps, _, ex, notif = make_deps()
    result = run(params, envelope(), deps)
    assert result.status is Status.POLICY_REJECTED
    assert len(audit_inserts(ex)) == 1
    assert len(notif.sent) == 1


def test_notifier_failure_writes_single_audit_row():
    """Notifier failure on success path should result in DRIVER_ERROR with one audit."""
    class RaisingNotifier:
        def send(self, to, subject, body):
            raise RuntimeError("notifier is down")

    deps, _, ex, _ = make_deps()
    deps.notifier = RaisingNotifier()
    result = run(make_params(), envelope(), deps)
    assert result.status is Status.DRIVER_ERROR
    assert len(audit_inserts(ex)) == 1
    # run() should not raise despite notifier failure


def test_connector_error_with_failing_notifier_single_audit_row():
    """ConnectorError with failing notifier should write single audit, preserve
    the connector's own status (not masked as DRIVER_ERROR), and not raise."""
    class RaisingNotifier:
        def send(self, to, subject, body):
            raise RuntimeError("notifier is down")

    conn = FakeConnector(read_error=ConnectorError(
        Status.VOLUME_EXCEEDED, "too big"))
    deps, _, ex, _ = make_deps(connector=conn)
    deps.notifier = RaisingNotifier()
    result = run(make_params(), envelope(), deps)
    assert result.status is Status.VOLUME_EXCEEDED
    assert len(audit_inserts(ex)) == 1
    # run() should not raise despite connector error + notifier failure
