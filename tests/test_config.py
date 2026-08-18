from migration_swamp.config import AUDIT_TABLE, SOURCES
from migration_swamp.status import Status


def test_status_members():
    assert {s.value for s in Status} == {
        "SUCCEEDED", "AUTH_FAILED", "ASSET_NOT_FOUND",
        "VOLUME_EXCEEDED", "POLICY_REJECTED", "NETWORK_BLOCKED",
        "DRIVER_ERROR",
    }


def test_source_registry():
    assert set(SOURCES) == {"sql_server", "oracle", "sas"}
    assert SOURCES["sql_server"].requires_dbhost is True
    assert SOURCES["oracle"].requires_dbhost is False
    assert SOURCES["sas"].requires_dbhost is False
    assert SOURCES["sas"].max_rows is not None  # SAS volume guardrail
    assert AUDIT_TABLE.count(".") == 2  # catalog.schema.table


def test_load_public_key_env_wins(monkeypatch):
    from migration_swamp.config import load_public_key
    monkeypatch.setenv("SWAMP_PUBLIC_KEY_PEM", "PEM-FROM-ENV")
    assert load_public_key() == "PEM-FROM-ENV"


def test_load_public_key_dev_fallback(monkeypatch):
    from migration_swamp.config import load_public_key
    monkeypatch.delenv("SWAMP_PUBLIC_KEY_PEM", raising=False)
    assert "BEGIN PUBLIC KEY" in load_public_key()
