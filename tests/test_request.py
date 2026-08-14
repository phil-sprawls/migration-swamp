import pytest

from migration_swamp.request import (
    AcquisitionRequest, RequestError, from_params, to_params, validate,
)


def make(**kw):
    base = dict(request_id="r1", requester="user@example.com",
                source_system="sql_server", schema="prod_db", table="data_table",
                dbhost="sqlhost01", gain_access=True, refresh=False)
    base.update(kw)
    return AcquisitionRequest(**base)


def test_valid_request_passes():
    validate(make())


def test_unknown_source_rejected():
    with pytest.raises(RequestError, match="source_system"):
        validate(make(source_system="mainframe"))


def test_dbhost_required_only_for_sql_server():
    with pytest.raises(RequestError, match="dbhost"):
        validate(make(dbhost=None))
    validate(make(source_system="oracle", dbhost=None))


def test_at_least_one_action_required():
    with pytest.raises(RequestError, match="action"):
        validate(make(gain_access=False, refresh=False))


def test_identifiers_must_be_sane():
    with pytest.raises(RequestError, match="table"):
        validate(make(table="t; DROP TABLE x"))
    with pytest.raises(RequestError, match="schema"):
        validate(make(schema=""))


def test_all_errors_reported_at_once():
    with pytest.raises(RequestError) as e:
        validate(make(source_system="mainframe", table="", gain_access=False,
                      refresh=False))
    msg = str(e.value)
    assert "source_system" in msg and "table" in msg and "action" in msg


def test_param_round_trip():
    req = make(refresh=True)
    assert from_params(to_params(req)) == req


def test_round_trip_none_dbhost():
    req = make(source_system="oracle", dbhost=None)
    assert from_params(to_params(req)) == req
