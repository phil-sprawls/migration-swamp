# migration-swamp v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the tested pure-Python core + thin Databricks glue for the self-service data acquisition system described in `docs/superpowers/specs/2026-08-14-migration-swamp-design.md`.

**Architecture:** A user-plane notebook (widgets + interactive auth + asset probe) triggers a gated Databricks job with an envelope-encrypted credential payload; the job re-probes as the user, full-replace-copies the asset to `<source_system>.<schema>.<table>`, tags it, grants the requester SELECT, writes an audit row, and emails the requester. All orchestration/validation/SQL-building/crypto is pure Python tested locally with pytest; JDBC/saspy/Spark glue is thin, lazily imported, and hardened at work.

**Tech Stack:** Python ≥3.10, `cryptography` (DBR built-in), pytest (dev only). No pyspark/saspy needed for local tests — connectors and SQL execution are faked.

## Global Constraints

- Runtime code uses ONLY Databricks Runtime built-in libraries (`cryptography` qualifies; `pyspark`/`saspy`/`databricks.sdk` are imported lazily inside glue functions only).
- Credentials NEVER appear in widgets, logs, error messages, audit rows, or job parameters (only ciphertext travels as a job parameter).
- Target path is always `<source_system>.<schema>.<table>` (e.g. `sql_server.prod_db.data_table`); tags always include `source_system` and `acquisition_type=copy`.
- `dbhost` is required iff source is `sql_server`.
- Copy-missing → pull even if user selected access-only; refresh → pull; exists+access-only → grant only.
- The exact copy-start message is: `Data copy started. You may close this notebook. You will receive an email when the data is ready.`
- Every job run writes exactly one audit row; statuses: `SUCCEEDED, AUTH_FAILED, ASSET_NOT_FOUND, VOLUME_EXCEEDED, POLICY_REJECTED, DRIVER_ERROR`.
- Each commit message ends with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: Project scaffold

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `migration_swamp/__init__.py`, `migration_swamp/connectors/__init__.py`, `tests/__init__.py`, `tests/test_scaffold.py`

**Interfaces:**
- Produces: installable package `migration_swamp` with `__version__ = "0.1.0"`; `pytest` runs green.

- [ ] **Step 1: Write files**

`pyproject.toml`:
```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[project]
name = "migration-swamp"
version = "0.1.0"
description = "Self-service data acquisition for Databricks"
requires-python = ">=3.10"
dependencies = ["cryptography>=41"]

[project.optional-dependencies]
dev = ["pytest>=7"]

[tool.setuptools.packages.find]
include = ["migration_swamp*"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

`.gitignore`:
```
__pycache__/
*.egg-info/
.venv/
.pytest_cache/
```

`migration_swamp/__init__.py`:
```python
__version__ = "0.1.0"
```

`migration_swamp/connectors/__init__.py`: empty. `tests/__init__.py`: empty.

`tests/test_scaffold.py`:
```python
import migration_swamp


def test_version():
    assert migration_swamp.__version__ == "0.1.0"
```

- [ ] **Step 2: Create venv, install, run tests**

Run: `python3 -m venv .venv && .venv/bin/pip install -e ".[dev]" -q && .venv/bin/pytest -q`
Expected: 1 passed

- [ ] **Step 3: Commit**

```bash
git add -A && git commit -m "feat: project scaffold"
```

---

### Task 2: Status enum and source registry (config)

**Files:**
- Create: `migration_swamp/status.py`, `migration_swamp/config.py`, `tests/test_config.py`

**Interfaces:**
- Produces: `Status` (str Enum) with members `SUCCEEDED, AUTH_FAILED, ASSET_NOT_FOUND, VOLUME_EXCEEDED, POLICY_REJECTED, DRIVER_ERROR`; `SourceConfig(name, requires_dbhost, endpoint, max_rows)`; `SOURCES: dict[str, SourceConfig]` with keys `sql_server, oracle, sas`; `AUDIT_TABLE: str`; `JOB_NAME: str`.

- [ ] **Step 1: Write the failing test** (`tests/test_config.py`)

```python
from migration_swamp.config import AUDIT_TABLE, SOURCES
from migration_swamp.status import Status


def test_status_members():
    assert {s.value for s in Status} == {
        "SUCCEEDED", "AUTH_FAILED", "ASSET_NOT_FOUND",
        "VOLUME_EXCEEDED", "POLICY_REJECTED", "DRIVER_ERROR",
    }


def test_source_registry():
    assert set(SOURCES) == {"sql_server", "oracle", "sas"}
    assert SOURCES["sql_server"].requires_dbhost is True
    assert SOURCES["oracle"].requires_dbhost is False
    assert SOURCES["sas"].requires_dbhost is False
    assert SOURCES["sas"].max_rows is not None  # SAS volume guardrail
    assert AUDIT_TABLE.count(".") == 2  # catalog.schema.table
```

- [ ] **Step 2: Run to verify it fails** — `.venv/bin/pytest tests/test_config.py -q` → ImportError.

- [ ] **Step 3: Implement**

`migration_swamp/status.py`:
```python
from enum import Enum


class Status(str, Enum):
    SUCCEEDED = "SUCCEEDED"
    AUTH_FAILED = "AUTH_FAILED"
    ASSET_NOT_FOUND = "ASSET_NOT_FOUND"
    VOLUME_EXCEEDED = "VOLUME_EXCEEDED"
    POLICY_REJECTED = "POLICY_REJECTED"
    DRIVER_ERROR = "DRIVER_ERROR"
```

`migration_swamp/config.py`:
```python
"""Deployment configuration. Values below are laptop defaults; the work
hardening pass replaces endpoints, AUDIT_TABLE, and JOB_NAME with
tenant-approved values."""
from dataclasses import dataclass


@dataclass(frozen=True)
class SourceConfig:
    name: str
    requires_dbhost: bool   # True: user must supply dbhost (SQL Server)
    endpoint: str | None    # fixed endpoint for oracle/sas; None if per-request
    max_rows: int | None    # volume guardrail (SAS); None = no limit


SOURCES: dict[str, SourceConfig] = {
    "sql_server": SourceConfig("sql_server", True, None, None),
    "oracle": SourceConfig("oracle", False, "oracle.example.internal:1521/ORCL", None),
    "sas": SourceConfig("sas", False, "sas.example.internal", 5_000_000),
}

AUDIT_TABLE = "swamp_meta.ops.acquisition_log"
JOB_NAME = "migration-swamp-acquire"
```

- [ ] **Step 4: Run tests** — `.venv/bin/pytest -q` → all pass.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: status enum and source registry"`

---

### Task 3: AcquisitionRequest + validation + param round-trip

**Files:**
- Create: `migration_swamp/request.py`, `tests/test_request.py`

**Interfaces:**
- Consumes: `config.SOURCES`.
- Produces: `AcquisitionRequest(request_id, requester, source_system, schema, table, dbhost, gain_access, refresh)` frozen dataclass; `RequestError(ValueError)`; `validate(req, sources=SOURCES) -> None` (raises `RequestError` listing ALL problems); `to_params(req) -> dict[str, str]`; `from_params(d: dict[str, str]) -> AcquisitionRequest`.

- [ ] **Step 1: Write the failing test** (`tests/test_request.py`)

```python
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
```

- [ ] **Step 2: Run to verify it fails** — `.venv/bin/pytest tests/test_request.py -q` → ImportError.

- [ ] **Step 3: Implement** (`migration_swamp/request.py`)

```python
import re
from dataclasses import asdict, dataclass

from migration_swamp.config import SOURCES

# Conservative on purpose: these values are interpolated into SQL and UC paths.
IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_$#.\-]{1,128}$")


class RequestError(ValueError):
    pass


@dataclass(frozen=True)
class AcquisitionRequest:
    request_id: str
    requester: str
    source_system: str
    schema: str
    table: str
    dbhost: str | None
    gain_access: bool
    refresh: bool


def validate(req: AcquisitionRequest, sources=SOURCES) -> None:
    errors: list[str] = []
    src = sources.get(req.source_system)
    if src is None:
        errors.append(f"source_system must be one of {sorted(sources)}")
    elif src.requires_dbhost and not req.dbhost:
        errors.append(f"dbhost is required for {req.source_system}")
    for field in ("schema", "table"):
        value = getattr(req, field)
        if not value or not IDENTIFIER_RE.match(value):
            errors.append(f"{field} must match {IDENTIFIER_RE.pattern}")
    if not (req.gain_access or req.refresh):
        errors.append("select at least one action (gain access / refresh data)")
    if not req.requester:
        errors.append("requester is required")
    if not req.request_id:
        errors.append("request_id is required")
    if errors:
        raise RequestError("; ".join(errors))


def to_params(req: AcquisitionRequest) -> dict[str, str]:
    d = asdict(req)
    d["dbhost"] = d["dbhost"] or ""
    d["gain_access"] = "true" if d["gain_access"] else "false"
    d["refresh"] = "true" if d["refresh"] else "false"
    return d


def from_params(d: dict[str, str]) -> AcquisitionRequest:
    return AcquisitionRequest(
        request_id=d["request_id"],
        requester=d["requester"],
        source_system=d["source_system"],
        schema=d["schema"],
        table=d["table"],
        dbhost=d["dbhost"] or None,
        gain_access=d["gain_access"] == "true",
        refresh=d["refresh"] == "true",
    )
```

- [ ] **Step 4: Run tests** — `.venv/bin/pytest -q` → all pass.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: acquisition request model and validation"`

---

### Task 4: Naming — sanitize + deterministic target path

**Files:**
- Create: `migration_swamp/naming.py`, `tests/test_naming.py`

**Interfaces:**
- Produces: `sanitize_identifier(raw: str) -> str`; `TargetPath(catalog, schema, table)` frozen dataclass with `.fqn` property returning backtick-quoted `` `c`.`s`.`t` `` and `.display` returning `c.s.t`; `target_path(source_system: str, schema: str, table: str) -> TargetPath`.

- [ ] **Step 1: Write the failing test** (`tests/test_naming.py`)

```python
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
```

- [ ] **Step 2: Run to verify it fails** — `.venv/bin/pytest tests/test_naming.py -q` → ImportError.

- [ ] **Step 3: Implement** (`migration_swamp/naming.py`)

```python
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class TargetPath:
    catalog: str
    schema: str
    table: str

    @property
    def fqn(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{self.table}`"

    @property
    def display(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


def sanitize_identifier(raw: str) -> str:
    s = re.sub(r"[^a-z0-9_]+", "_", raw.lower()).strip("_")
    s = re.sub(r"_+", "_", s)
    if s and s[0].isdigit():
        s = f"t_{s}"
    return s


def target_path(source_system: str, schema: str, table: str) -> TargetPath:
    return TargetPath(
        sanitize_identifier(source_system),
        sanitize_identifier(schema),
        sanitize_identifier(table),
    )
```

- [ ] **Step 4: Run tests** — `.venv/bin/pytest -q` → all pass.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: naming sanitization and deterministic target path"`

---

### Task 5: Envelope encryption (crypto)

**Files:**
- Create: `migration_swamp/crypto.py`, `tests/test_crypto.py`

**Interfaces:**
- Produces: `Credentials(username, password)` frozen dataclass; `generate_keypair() -> tuple[str, str]` (private_pem, public_pem); `encrypt_credentials(creds: Credentials, public_pem: str) -> str` (base64 envelope safe as a job parameter); `decrypt_credentials(envelope: str, private_pem: str) -> Credentials`; `CryptoError(Exception)`.

- [ ] **Step 1: Write the failing test** (`tests/test_crypto.py`)

```python
import pytest

from migration_swamp.crypto import (
    Credentials, CryptoError, decrypt_credentials, encrypt_credentials,
    generate_keypair,
)


def test_round_trip():
    priv, pub = generate_keypair()
    creds = Credentials("phil", "s3cret!pw")
    envelope = encrypt_credentials(creds, pub)
    assert decrypt_credentials(envelope, priv) == creds


def test_envelope_is_opaque_ascii():
    _, pub = generate_keypair()
    envelope = encrypt_credentials(Credentials("phil", "s3cret!pw"), pub)
    assert "phil" not in envelope and "s3cret!pw" not in envelope
    envelope.encode("ascii")  # job parameters must be plain text


def test_tampered_envelope_rejected():
    priv, pub = generate_keypair()
    envelope = encrypt_credentials(Credentials("u", "p"), pub)
    tampered = envelope[:-4] + ("AAAA" if envelope[-4:] != "AAAA" else "BBBB")
    with pytest.raises(CryptoError):
        decrypt_credentials(tampered, priv)


def test_wrong_key_rejected():
    _, pub = generate_keypair()
    other_priv, _ = generate_keypair()
    envelope = encrypt_credentials(Credentials("u", "p"), pub)
    with pytest.raises(CryptoError):
        decrypt_credentials(envelope, other_priv)
```

- [ ] **Step 2: Run to verify it fails** — `.venv/bin/pytest tests/test_crypto.py -q` → ImportError.

- [ ] **Step 3: Implement** (`migration_swamp/crypto.py`)

```python
import base64
import json
import os
from dataclasses import dataclass

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class CryptoError(Exception):
    pass


@dataclass(frozen=True)
class Credentials:
    username: str
    password: str


_OAEP = padding.OAEP(
    mgf=padding.MGF1(algorithm=hashes.SHA256()),
    algorithm=hashes.SHA256(),
    label=None,
)


def generate_keypair() -> tuple[str, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
    private_pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    public_pem = key.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    return private_pem, public_pem


def encrypt_credentials(creds: Credentials, public_pem: str) -> str:
    public_key = serialization.load_pem_public_key(public_pem.encode())
    data_key = AESGCM.generate_key(bit_length=256)
    nonce = os.urandom(12)
    plaintext = json.dumps(
        {"username": creds.username, "password": creds.password}
    ).encode()
    ciphertext = AESGCM(data_key).encrypt(nonce, plaintext, None)
    envelope = {
        "k": base64.b64encode(public_key.encrypt(data_key, _OAEP)).decode(),
        "n": base64.b64encode(nonce).decode(),
        "c": base64.b64encode(ciphertext).decode(),
    }
    return base64.b64encode(json.dumps(envelope).encode()).decode()


def decrypt_credentials(envelope: str, private_pem: str) -> Credentials:
    try:
        private_key = serialization.load_pem_private_key(
            private_pem.encode(), password=None
        )
        outer = json.loads(base64.b64decode(envelope))
        data_key = private_key.decrypt(base64.b64decode(outer["k"]), _OAEP)
        plaintext = AESGCM(data_key).decrypt(
            base64.b64decode(outer["n"]), base64.b64decode(outer["c"]), None
        )
        inner = json.loads(plaintext)
        return Credentials(inner["username"], inner["password"])
    except Exception as exc:  # noqa: BLE001 - single failure surface by design
        raise CryptoError("credential envelope could not be decrypted") from exc
```

- [ ] **Step 4: Run tests** — `.venv/bin/pytest -q` → all pass.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: envelope encryption for credential handoff"`

---

### Task 6: Secret scrubbing + interactive auth prompt

**Files:**
- Create: `migration_swamp/scrub.py`, `migration_swamp/interactive_auth.py`, `tests/test_scrub.py`, `tests/test_interactive_auth.py`

**Interfaces:**
- Consumes: `crypto.Credentials`.
- Produces: `scrub(text: str, secrets: Sequence[str]) -> str` (replaces every secret occurrence with `***`; ignores empty secrets); `prompt_credentials(source_system: str, prompt=input, secret=getpass.getpass) -> Credentials`.

- [ ] **Step 1: Write the failing tests**

`tests/test_scrub.py`:
```python
from migration_swamp.scrub import scrub


def test_scrub_replaces_all_occurrences():
    out = scrub("login failed for phil with pw=hunter2 (hunter2)",
                ["hunter2", "phil"])
    assert "hunter2" not in out and "phil" not in out
    assert out.count("***") == 3


def test_scrub_ignores_empty_secrets():
    assert scrub("hello", ["", "hello"]) == "***"
```

`tests/test_interactive_auth.py`:
```python
from migration_swamp.crypto import Credentials
from migration_swamp.interactive_auth import prompt_credentials


def test_prompt_uses_secret_input_for_password():
    calls = []

    def fake_prompt(msg):
        calls.append(("prompt", msg))
        return "phil"

    def fake_secret(msg):
        calls.append(("secret", msg))
        return "pw"

    creds = prompt_credentials("oracle", prompt=fake_prompt, secret=fake_secret)
    assert creds == Credentials("phil", "pw")
    assert [kind for kind, _ in calls] == ["prompt", "secret"]
    assert "oracle" in calls[0][1]
```

- [ ] **Step 2: Run to verify they fail** — `.venv/bin/pytest tests/test_scrub.py tests/test_interactive_auth.py -q` → ImportError.

- [ ] **Step 3: Implement**

`migration_swamp/scrub.py`:
```python
from collections.abc import Sequence


def scrub(text: str, secrets: Sequence[str]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text
```

`migration_swamp/interactive_auth.py`:
```python
"""Interactive credential collection. This module is the single swap point
for the company's interactive-auth pattern during work hardening. Credentials
are returned in memory only - never widgets, never disk."""
import getpass

from migration_swamp.crypto import Credentials


def prompt_credentials(source_system: str, prompt=input,
                       secret=getpass.getpass) -> Credentials:
    username = prompt(
        f"[{source_system}] username (used only to verify your access): "
    )
    password = secret(f"[{source_system}] password (never stored): ")
    return Credentials(username, password)
```

- [ ] **Step 4: Run tests** — `.venv/bin/pytest -q` → all pass.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: secret scrubbing and interactive auth prompt"`

---

### Task 7: Connector contract (base) + governance SQL builders

**Files:**
- Create: `migration_swamp/connectors/base.py`, `migration_swamp/governance.py`, `tests/test_governance.py`

**Interfaces:**
- Consumes: `Status`, `TargetPath`, `AcquisitionRequest`, `Credentials`.
- Produces:
  - `Asset(source_system, schema, table, dbhost=None)` frozen dataclass; `ProbeResult(ok: bool, status: Status | None = None, message: str = "")` frozen dataclass; `Connector` Protocol with `probe(asset, creds) -> ProbeResult` and `read_to_staging(asset, creds, staging_view: str) -> int` (row count; raises `ConnectorError(status, message)` on failure).
  - `governance.build_create_table(target: TargetPath, staging_view: str) -> str`; `build_set_tags(target, req: AcquisitionRequest, row_count: int | None, acquired_at: str) -> str`; `build_grant_select(target, principal: str) -> str`.

- [ ] **Step 1: Write the failing test** (`tests/test_governance.py`)

```python
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
```

- [ ] **Step 2: Run to verify it fails** — `.venv/bin/pytest tests/test_governance.py -q` → ImportError.

- [ ] **Step 3: Implement**

`migration_swamp/connectors/base.py`:
```python
from dataclasses import dataclass
from typing import Protocol

from migration_swamp.crypto import Credentials
from migration_swamp.status import Status


@dataclass(frozen=True)
class Asset:
    source_system: str
    schema: str
    table: str
    dbhost: str | None = None


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    status: Status | None = None
    message: str = ""


class ConnectorError(Exception):
    def __init__(self, status: Status, message: str):
        super().__init__(message)
        self.status = status


class Connector(Protocol):
    def probe(self, asset: Asset, creds: Credentials) -> ProbeResult:
        """Cheap table-level entitlement check (SELECT ... WHERE 1=0)."""

    def read_to_staging(self, asset: Asset, creds: Credentials,
                        staging_view: str) -> int:
        """Full-read the asset into a temp view; return row count.
        Raises ConnectorError on failure."""
```

`migration_swamp/governance.py`:
```python
from migration_swamp.naming import TargetPath
from migration_swamp.request import AcquisitionRequest


def _q(value: str) -> str:
    return value.replace("'", "''")


def build_create_table(target: TargetPath, staging_view: str) -> str:
    return (f"CREATE OR REPLACE TABLE {target.fqn} "
            f"AS SELECT * FROM {staging_view}")


def build_set_tags(target: TargetPath, req: AcquisitionRequest,
                   row_count: int | None, acquired_at: str) -> str:
    tags = {
        "source_system": req.source_system,
        "acquisition_type": "copy",
        "source_schema": req.schema,
        "source_table": req.table,
        "acquired_by": req.requester,
        "acquired_at": acquired_at,
    }
    if req.dbhost:
        tags["source_host"] = req.dbhost
    if row_count is not None:
        tags["row_count"] = str(row_count)
    pairs = ", ".join(f"'{_q(k)}' = '{_q(v)}'" for k, v in tags.items())
    return f"ALTER TABLE {target.fqn} SET TAGS ({pairs})"


def build_grant_select(target: TargetPath, principal: str) -> str:
    return f"GRANT SELECT ON TABLE {target.fqn} TO `{principal}`"
```

- [ ] **Step 4: Run tests** — `.venv/bin/pytest -q` → all pass.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: connector contract and governance SQL builders"`

---

### Task 8: Audit log builders

**Files:**
- Create: `migration_swamp/audit.py`, `tests/test_audit.py`

**Interfaces:**
- Consumes: `Status`, `TargetPath`, `AcquisitionRequest`, `config.AUDIT_TABLE`.
- Produces: `build_ensure_table(audit_table: str = AUDIT_TABLE) -> str`; `build_row(req, target, status: Status, row_count, started_at: str, finished_at: str, hint: str) -> dict`; `build_insert(row: dict, audit_table: str = AUDIT_TABLE) -> str`. Row keys (fixed order): `request_id, requester, source_system, source_schema, source_table, dbhost, gain_access, refresh, target_table, status, row_count, started_at, finished_at, hint`.

- [ ] **Step 1: Write the failing test** (`tests/test_audit.py`)

```python
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
```

- [ ] **Step 2: Run to verify it fails** — `.venv/bin/pytest tests/test_audit.py -q` → ImportError.

- [ ] **Step 3: Implement** (`migration_swamp/audit.py`)

```python
from migration_swamp.config import AUDIT_TABLE
from migration_swamp.naming import TargetPath
from migration_swamp.request import AcquisitionRequest
from migration_swamp.status import Status

COLUMNS = [
    ("request_id", "STRING"), ("requester", "STRING"),
    ("source_system", "STRING"), ("source_schema", "STRING"),
    ("source_table", "STRING"), ("dbhost", "STRING"),
    ("gain_access", "BOOLEAN"), ("refresh", "BOOLEAN"),
    ("target_table", "STRING"), ("status", "STRING"),
    ("row_count", "BIGINT"), ("started_at", "STRING"),
    ("finished_at", "STRING"), ("hint", "STRING"),
]


def build_ensure_table(audit_table: str = AUDIT_TABLE) -> str:
    cols = ", ".join(f"{name} {typ}" for name, typ in COLUMNS)
    return f"CREATE TABLE IF NOT EXISTS {audit_table} ({cols})"


def build_row(req: AcquisitionRequest, target: TargetPath | None,
              status: Status, row_count: int | None, started_at: str,
              finished_at: str, hint: str) -> dict:
    return {
        "request_id": req.request_id,
        "requester": req.requester,
        "source_system": req.source_system,
        "source_schema": req.schema,
        "source_table": req.table,
        "dbhost": req.dbhost,
        "gain_access": req.gain_access,
        "refresh": req.refresh,
        "target_table": target.display if target else None,
        "status": status.value,
        "row_count": row_count,
        "started_at": started_at,
        "finished_at": finished_at,
        "hint": hint,
    }


def _literal(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def build_insert(row: dict, audit_table: str = AUDIT_TABLE) -> str:
    names = ", ".join(name for name, _ in COLUMNS)
    values = ", ".join(_literal(row[name]) for name, _ in COLUMNS)
    return f"INSERT INTO {audit_table} ({names}) VALUES ({values})"
```

- [ ] **Step 4: Run tests** — `.venv/bin/pytest -q` → all pass.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: audit log builders"`

---

### Task 9: User messages + notification composition

**Files:**
- Create: `migration_swamp/messages.py`, `migration_swamp/notify.py`, `tests/test_messages.py`, `tests/test_notify.py`

**Interfaces:**
- Consumes: `Status`, `TargetPath`, `AcquisitionRequest`.
- Produces:
  - `messages.COPY_STARTED` — the exact spec string; `WELCOME: str`; `probing(display: str) -> str`; `probe_ok(display: str) -> str`; `probe_failed(reason: str) -> str`; `access_only_submitted(display: str) -> str`; `HINTS: dict[Status, str]` (entry for every non-SUCCEEDED status).
  - `notify.Notifier` Protocol with `send(to: str, subject: str, body: str) -> None`; `LoggingNotifier` (records `.sent: list[tuple[str, str, str]]` and prints); `compose_success(req, target, row_count) -> tuple[str, str]`; `compose_failure(req, status, hint) -> tuple[str, str]`.

- [ ] **Step 1: Write the failing tests**

`tests/test_messages.py`:
```python
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
```

`tests/test_notify.py`:
```python
from migration_swamp.naming import target_path
from migration_swamp.notify import LoggingNotifier, compose_failure, compose_success
from migration_swamp.request import AcquisitionRequest
from migration_swamp.status import Status

REQ = AcquisitionRequest("r1", "user@example.com", "sql_server", "prod_db",
                         "data_table", "sqlhost01", True, True)
TP = target_path("sql_server", "prod_db", "data_table")


def test_success_email_has_path_and_rows():
    subject, body = compose_success(REQ, TP, 42)
    assert "ready" in subject.lower()
    assert "sql_server.prod_db.data_table" in body
    assert "42" in body
    assert "serverless" in body.lower()  # how to query next


def test_failure_email_has_status_and_hint():
    subject, body = compose_failure(REQ, Status.AUTH_FAILED, "check password")
    assert "failed" in subject.lower()
    assert "AUTH_FAILED" in body and "check password" in body
    assert "r1" in body  # request id for support


def test_logging_notifier_records():
    n = LoggingNotifier()
    n.send("user@example.com", "s", "b")
    assert n.sent == [("user@example.com", "s", "b")]
```

- [ ] **Step 2: Run to verify they fail** — `.venv/bin/pytest tests/test_messages.py tests/test_notify.py -q` → ImportError.

- [ ] **Step 3: Implement**

`migration_swamp/messages.py`:
```python
from migration_swamp.status import Status

WELCOME = (
    "migration-swamp: self-service data acquisition.\n"
    "Pick a source, name the asset, verify your access, and a governed copy\n"
    "lands in Unity Catalog. You will need your source-system credentials."
)

COPY_STARTED = (
    "Data copy started. You may close this notebook. "
    "You will receive an email when the data is ready."
)


def probing(display: str) -> str:
    return f"Verifying you can read {display} on the source system..."


def probe_ok(display: str) -> str:
    return f"Access verified for {display}. Submitting your request."


def probe_failed(reason: str) -> str:
    return (f"Could not verify access: {reason}\n"
            "The flow stops here. Fix the issue above and re-run this cell.")


def access_only_submitted(display: str) -> str:
    return (f"Access request submitted for {display}. "
            "You will receive an email when your access is granted.")


HINTS: dict[Status, str] = {
    Status.AUTH_FAILED: (
        "Check your username/password and confirm your on-prem account can "
        "read this asset."),
    Status.ASSET_NOT_FOUND: (
        "Verify the schema and table names exist on the source system."),
    Status.VOLUME_EXCEEDED: (
        "This SAS dataset exceeds the transfer guardrail. Contact the "
        "platform team for a bulk load."),
    Status.POLICY_REJECTED: (
        "The request failed validation. Correct the inputs and resubmit."),
    Status.DRIVER_ERROR: (
        "Unexpected connector error. Contact the platform team with your "
        "request id."),
}
```

`migration_swamp/notify.py`:
```python
from typing import Protocol

from migration_swamp.naming import TargetPath
from migration_swamp.request import AcquisitionRequest
from migration_swamp.status import Status


class Notifier(Protocol):
    def send(self, to: str, subject: str, body: str) -> None: ...


class LoggingNotifier:
    """Laptop/default notifier. Work hardening swaps in the approved
    company email pattern behind the same send() signature."""

    def __init__(self):
        self.sent: list[tuple[str, str, str]] = []

    def send(self, to: str, subject: str, body: str) -> None:
        self.sent.append((to, subject, body))
        print(f"[notify] to={to} subject={subject}\n{body}")


def compose_success(req: AcquisitionRequest, target: TargetPath,
                    row_count: int | None) -> tuple[str, str]:
    subject = f"Your data is ready: {target.display}"
    rows = f"{row_count} rows copied." if row_count is not None else \
        "Access granted to the existing copy (no new copy needed)."
    body = (
        f"Request {req.request_id} succeeded.\n\n"
        f"Table: {target.display}\n{rows}\n\n"
        f"Query it from any serverless SQL warehouse:\n"
        f"  SELECT * FROM {target.display} LIMIT 100\n\n"
        f"To refresh this data later, re-run the Acquire notebook with "
        f"'Refresh data' selected."
    )
    return subject, body


def compose_failure(req: AcquisitionRequest, status: Status,
                    hint: str) -> tuple[str, str]:
    subject = f"Data acquisition failed: {req.schema}.{req.table}"
    body = (
        f"Request {req.request_id} failed with status {status.value}.\n\n"
        f"What to do: {hint}\n\n"
        f"Source: {req.source_system} {req.schema}.{req.table}\n"
        f"If you need help, contact the platform team and include request "
        f"id {req.request_id}."
    )
    return subject, body
```

- [ ] **Step 4: Run tests** — `.venv/bin/pytest -q` → all pass.

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: user messages and notification composition"`

---

### Task 10: Executor contract + job orchestration (the pull plane)

**Files:**
- Create: `migration_swamp/executor.py`, `migration_swamp/job.py`, `tests/fakes.py`, `tests/test_job.py`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `executor.SqlExecutor` Protocol: `execute(sql: str) -> None`; `table_exists(target: TargetPath) -> bool`.
  - `job.JobDeps(connector_factory: Callable[[AcquisitionRequest], Connector], executor: SqlExecutor, notifier: Notifier, private_key_pem: str, now: Callable[[], str], audit_table: str = AUDIT_TABLE)` dataclass.
  - `job.JobResult(status: Status, target_display: str | None, row_count: int | None)` frozen dataclass.
  - `job.run(params: dict[str, str], envelope: str, deps: JobDeps) -> JobResult`.
  - `job.STAGING_VIEW = "_swamp_staging"`.
- Behavior contract (the decision matrix, verbatim from the spec):
  1. invalid params → `POLICY_REJECTED`; audit + failure email; no SQL writes.
  2. bad envelope → `POLICY_REJECTED` with re-run hint.
  3. probe failure → connector's status (`AUTH_FAILED`/`ASSET_NOT_FOUND`/…); audit + failure email; no writes.
  4. copy missing → pull + create + tags + grant + audit + success email — even if access-only.
  5. refresh selected → pull + create + tags + grant (idempotent re-grant) + audit + success email.
  6. exists + access-only → NO pull/create/tags; grant + audit (`row_count=None`) + success email ("no new copy needed").
  7. connector raises `ConnectorError` during read → its status; unexpected exception anywhere → `DRIVER_ERROR`; both audited + emailed, message scrubbed of username/password.
  8. exactly one audit row per run, in every path.

- [ ] **Step 1: Write fakes** (`tests/fakes.py`)

```python
from migration_swamp.connectors.base import Asset, ConnectorError, ProbeResult
from migration_swamp.crypto import Credentials
from migration_swamp.naming import TargetPath


class FakeConnector:
    def __init__(self, probe_result: ProbeResult = ProbeResult(ok=True),
                 rows: int = 42, read_error: ConnectorError | None = None):
        self.probe_result = probe_result
        self.rows = rows
        self.read_error = read_error
        self.probed_with: list[tuple[Asset, Credentials]] = []
        self.read_calls: list[tuple[Asset, str]] = []

    def probe(self, asset: Asset, creds: Credentials) -> ProbeResult:
        self.probed_with.append((asset, creds))
        return self.probe_result

    def read_to_staging(self, asset: Asset, creds: Credentials,
                        staging_view: str) -> int:
        self.read_calls.append((asset, staging_view))
        if self.read_error:
            raise self.read_error
        return self.rows


class FakeExecutor:
    def __init__(self, existing_tables: set[str] | None = None):
        self.existing = existing_tables or set()
        self.executed: list[str] = []

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def table_exists(self, target: TargetPath) -> bool:
        return target.display in self.existing
```

- [ ] **Step 2: Write the failing tests** (`tests/test_job.py`)

```python
import pytest

from migration_swamp.connectors.base import ConnectorError, ProbeResult
from migration_swamp.crypto import Credentials, encrypt_credentials, generate_keypair
from migration_swamp.job import JobDeps, run
from migration_swamp.notify import LoggingNotifier
from migration_swamp.request import AcquisitionRequest, to_params
from migration_swamp.status import Status
from tests.fakes import FakeConnector, FakeExecutor

PRIV, PUB = generate_keypair()
CREDS = Credentials("phil", "hunter2")
TARGET = "sql_server.prod_db.data_table"


def make_params(**kw):
    base = dict(request_id="r1", requester="user@example.com",
                source_system="sql_server", schema="prod_db",
                table="data_table", dbhost="sqlhost01",
                gain_access=True, refresh=False)
    base.update(kw)
    return to_params(AcquisitionRequest(**base))


def make_deps(connector=None, executor=None):
    connector = connector or FakeConnector()
    executor = executor if executor is not None else FakeExecutor()
    notifier = LoggingNotifier()
    deps = JobDeps(connector_factory=lambda req: connector,
                   executor=executor, notifier=notifier,
                   private_key_pem=PRIV, now=lambda: "2026-08-14T12:00:00Z")
    return deps, connector, executor, notifier


def envelope():
    return encrypt_credentials(CREDS, PUB)


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
    result = run(make_params(refresh=True), envelope(), deps)
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
```

- [ ] **Step 3: Run to verify they fail** — `.venv/bin/pytest tests/test_job.py -q` → ImportError.

- [ ] **Step 4: Implement**

`migration_swamp/executor.py`:
```python
from typing import Protocol

from migration_swamp.naming import TargetPath


class SqlExecutor(Protocol):
    def execute(self, sql: str) -> None: ...

    def table_exists(self, target: TargetPath) -> bool: ...


class SparkExecutor:
    """Real executor for the Databricks job. Lazy: no pyspark at import."""

    def __init__(self, spark):
        self._spark = spark

    def execute(self, sql: str) -> None:
        self._spark.sql(sql)

    def table_exists(self, target: TargetPath) -> bool:
        return self._spark.catalog.tableExists(target.display)
```

`migration_swamp/job.py`:
```python
from collections.abc import Callable
from dataclasses import dataclass

from migration_swamp import audit, governance, messages
from migration_swamp.config import AUDIT_TABLE
from migration_swamp.connectors.base import Asset, Connector, ConnectorError
from migration_swamp.crypto import CryptoError, decrypt_credentials
from migration_swamp.executor import SqlExecutor
from migration_swamp.naming import TargetPath, target_path
from migration_swamp.notify import Notifier, compose_failure, compose_success
from migration_swamp.request import (
    AcquisitionRequest, RequestError, from_params, validate,
)
from migration_swamp.scrub import scrub
from migration_swamp.status import Status

STAGING_VIEW = "_swamp_staging"


@dataclass
class JobDeps:
    connector_factory: Callable[[AcquisitionRequest], Connector]
    executor: SqlExecutor
    notifier: Notifier
    private_key_pem: str
    now: Callable[[], str]
    audit_table: str = AUDIT_TABLE


@dataclass(frozen=True)
class JobResult:
    status: Status
    target_display: str | None
    row_count: int | None


def run(params: dict[str, str], envelope: str, deps: JobDeps) -> JobResult:
    started_at = deps.now()
    req = from_params(params)
    secrets: list[str] = []
    target: TargetPath | None = None
    row_count: int | None = None

    def finish(status: Status, hint: str = "") -> JobResult:
        finished_at = deps.now()
        hint = hint or messages.HINTS.get(status, "")
        row = audit.build_row(req, target, status, row_count, started_at,
                              finished_at, scrub(hint, secrets))
        deps.executor.execute(audit.build_ensure_table(deps.audit_table))
        deps.executor.execute(audit.build_insert(row, deps.audit_table))
        if status is Status.SUCCEEDED:
            subject, body = compose_success(req, target, row_count)
        else:
            subject, body = compose_failure(req, status, scrub(hint, secrets))
        deps.notifier.send(req.requester, scrub(subject, secrets),
                           scrub(body, secrets))
        return JobResult(status, target.display if target else None, row_count)

    try:
        try:
            validate(req)
        except RequestError as exc:
            return finish(Status.POLICY_REJECTED, str(exc))

        target = target_path(req.source_system, req.schema, req.table)

        try:
            creds = decrypt_credentials(envelope, deps.private_key_pem)
        except CryptoError:
            target = None
            return finish(
                Status.POLICY_REJECTED,
                "The credential payload could not be read. Re-run the "
                "Acquire notebook to submit a fresh request.")
        secrets = [creds.username, creds.password]

        connector = deps.connector_factory(req)
        asset = Asset(req.source_system, req.schema, req.table, req.dbhost)

        probe = connector.probe(asset, creds)
        if not probe.ok:
            return finish(probe.status or Status.DRIVER_ERROR,
                          messages.HINTS.get(
                              probe.status or Status.DRIVER_ERROR, ""))

        exists = deps.executor.table_exists(target)
        need_pull = (not exists) or req.refresh

        if need_pull:
            row_count = connector.read_to_staging(asset, creds, STAGING_VIEW)
            deps.executor.execute(
                governance.build_create_table(target, STAGING_VIEW))
            deps.executor.execute(
                governance.build_set_tags(target, req, row_count, deps.now()))

        deps.executor.execute(
            governance.build_grant_select(target, req.requester))
        return finish(Status.SUCCEEDED)

    except ConnectorError as exc:
        return finish(exc.status, scrub(str(exc), secrets))
    except Exception as exc:  # noqa: BLE001 - job must always audit+notify
        return finish(Status.DRIVER_ERROR,
                      messages.HINTS[Status.DRIVER_ERROR]
                      + " Detail: " + scrub(str(exc), secrets))
```

- [ ] **Step 5: Run tests** — `.venv/bin/pytest -q` → all pass.

- [ ] **Step 6: Commit** — `git add -A && git commit -m "feat: executor contract and gated job orchestration"`

---

### Task 11: Real connectors (thin glue, pure parts tested)

**Files:**
- Create: `migration_swamp/connectors/sqlserver.py`, `migration_swamp/connectors/oracle.py`, `migration_swamp/connectors/sas.py`, `migration_swamp/connectors/factory.py`, `tests/test_connectors.py`

**Interfaces:**
- Consumes: `Asset`, `ProbeResult`, `ConnectorError`, `Credentials`, `config.SOURCES`.
- Produces:
  - Pure/testable: `sqlserver.build_jdbc_url(dbhost: str) -> str`; `oracle.build_jdbc_url(endpoint: str) -> str`; `build_probe_query(schema: str, table: str) -> str` (in both JDBC modules, identical output `SELECT * FROM schema.table WHERE 1=0`); `classify_jdbc_error(message: str) -> Status` (login/auth text → `AUTH_FAILED`; object-not-found text → `ASSET_NOT_FOUND`; else `DRIVER_ERROR`) — one shared implementation in `sqlserver.py`, imported by `oracle.py`.
  - Glue (lazy imports, NOT unit-tested locally): `SqlServerConnector(spark)`, `OracleConnector(spark, endpoint)`, `SasConnector(spark, endpoint, max_rows)` — each implements the `Connector` protocol; SAS enforces `max_rows` by raising `ConnectorError(Status.VOLUME_EXCEEDED, ...)`.
  - `factory.make_connector(req: AcquisitionRequest, spark) -> Connector` — maps source_system to the right class using `config.SOURCES`.

- [ ] **Step 1: Write the failing test** (`tests/test_connectors.py`)

```python
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
```

- [ ] **Step 2: Run to verify it fails** — `.venv/bin/pytest tests/test_connectors.py -q` → ImportError.

- [ ] **Step 3: Implement**

`migration_swamp/connectors/sqlserver.py`:
```python
"""SQL Server connector. Pure helpers are unit-tested; the Connector class
is thin Spark/JDBC glue hardened at work against the approved pattern."""
from migration_swamp.connectors.base import (
    Asset, ConnectorError, ProbeResult,
)
from migration_swamp.crypto import Credentials
from migration_swamp.status import Status

_AUTH_MARKERS = ("login failed", "ora-01017", "authentication failed")
_NOT_FOUND_MARKERS = ("invalid object name", "ora-00942", "does not exist",
                      "not found")


def build_jdbc_url(dbhost: str) -> str:
    return f"jdbc:sqlserver://{dbhost};encrypt=true;trustServerCertificate=true"


def build_probe_query(schema: str, table: str) -> str:
    return f"SELECT * FROM {schema}.{table} WHERE 1=0"


def classify_jdbc_error(message: str) -> Status:
    lower = message.lower()
    if any(m in lower for m in _AUTH_MARKERS):
        return Status.AUTH_FAILED
    if any(m in lower for m in _NOT_FOUND_MARKERS):
        return Status.ASSET_NOT_FOUND
    return Status.DRIVER_ERROR


class SqlServerConnector:
    def __init__(self, spark):
        self._spark = spark

    def _reader(self, asset: Asset, creds: Credentials, query: str):
        return (self._spark.read.format("jdbc")
                .option("url", build_jdbc_url(asset.dbhost))
                .option("query", query)
                .option("user", creds.username)
                .option("password", creds.password))

    def probe(self, asset: Asset, creds: Credentials) -> ProbeResult:
        try:
            self._reader(asset, creds,
                         build_probe_query(asset.schema, asset.table)).load()
            return ProbeResult(ok=True)
        except Exception as exc:  # noqa: BLE001 - classified below
            return ProbeResult(ok=False, status=classify_jdbc_error(str(exc)),
                               message=str(exc))

    def read_to_staging(self, asset: Asset, creds: Credentials,
                        staging_view: str) -> int:
        try:
            df = self._reader(
                asset, creds,
                f"SELECT * FROM {asset.schema}.{asset.table}").load()
            df.createOrReplaceTempView(staging_view)
            return df.count()
        except Exception as exc:  # noqa: BLE001
            raise ConnectorError(classify_jdbc_error(str(exc)),
                                 str(exc)) from exc
```

`migration_swamp/connectors/oracle.py`:
```python
"""Oracle connector. Endpoint is fixed in config; same JDBC shape as
SQL Server with an Oracle URL."""
from migration_swamp.connectors.base import (
    Asset, ConnectorError, ProbeResult,
)
from migration_swamp.connectors.sqlserver import (
    build_probe_query, classify_jdbc_error,
)
from migration_swamp.crypto import Credentials


def build_jdbc_url(endpoint: str) -> str:
    return f"jdbc:oracle:thin:@//{endpoint}"


class OracleConnector:
    def __init__(self, spark, endpoint: str):
        self._spark = spark
        self._endpoint = endpoint

    def _reader(self, creds: Credentials, query: str):
        return (self._spark.read.format("jdbc")
                .option("url", build_jdbc_url(self._endpoint))
                .option("query", query)
                .option("user", creds.username)
                .option("password", creds.password))

    def probe(self, asset: Asset, creds: Credentials) -> ProbeResult:
        try:
            self._reader(creds,
                         build_probe_query(asset.schema, asset.table)).load()
            return ProbeResult(ok=True)
        except Exception as exc:  # noqa: BLE001
            return ProbeResult(ok=False, status=classify_jdbc_error(str(exc)),
                               message=str(exc))

    def read_to_staging(self, asset: Asset, creds: Credentials,
                        staging_view: str) -> int:
        try:
            df = self._reader(
                creds, f"SELECT * FROM {asset.schema}.{asset.table}").load()
            df.createOrReplaceTempView(staging_view)
            return df.count()
        except Exception as exc:  # noqa: BLE001
            raise ConnectorError(classify_jdbc_error(str(exc)),
                                 str(exc)) from exc
```

`migration_swamp/connectors/sas.py`:
```python
"""SAS connector via saspy (lazy import - saspy exists only on the work
cluster). schema maps to the SAS libref, table to the dataset name.
Enforces the volume guardrail from config."""
from migration_swamp.connectors.base import (
    Asset, ConnectorError, ProbeResult,
)
from migration_swamp.crypto import Credentials
from migration_swamp.status import Status


class SasConnector:
    def __init__(self, spark, endpoint: str, max_rows: int | None):
        self._spark = spark
        self._endpoint = endpoint
        self._max_rows = max_rows

    def _session(self, creds: Credentials):
        import saspy  # lazy: work-cluster only

        # Work hardening: replace with the approved saspy config pattern.
        return saspy.SASsession(
            iomhost=self._endpoint, iomport=8591,
            omruser=creds.username, omrpw=creds.password,
        )

    def probe(self, asset: Asset, creds: Credentials) -> ProbeResult:
        try:
            sas = self._session(creds)
            try:
                if not sas.exist(asset.table, libref=asset.schema):
                    return ProbeResult(ok=False,
                                       status=Status.ASSET_NOT_FOUND,
                                       message=f"{asset.schema}.{asset.table} "
                                               "not found")
                return ProbeResult(ok=True)
            finally:
                sas.endsas()
        except Exception as exc:  # noqa: BLE001 - auth/connect failures
            return ProbeResult(ok=False, status=Status.AUTH_FAILED,
                               message=str(exc))

    def read_to_staging(self, asset: Asset, creds: Credentials,
                        staging_view: str) -> int:
        sas = self._session(creds)
        try:
            data = sas.sasdata(asset.table, libref=asset.schema)
            obs = int(data.obs())
            if self._max_rows is not None and obs > self._max_rows:
                raise ConnectorError(
                    Status.VOLUME_EXCEEDED,
                    f"{asset.schema}.{asset.table} has {obs} rows; "
                    f"guardrail is {self._max_rows}")
            pdf = data.to_df()
            df = self._spark.createDataFrame(pdf)
            df.createOrReplaceTempView(staging_view)
            return obs
        except ConnectorError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise ConnectorError(Status.DRIVER_ERROR, str(exc)) from exc
        finally:
            sas.endsas()
```

`migration_swamp/connectors/factory.py`:
```python
from migration_swamp.config import SOURCES
from migration_swamp.connectors.oracle import OracleConnector
from migration_swamp.connectors.sas import SasConnector
from migration_swamp.connectors.sqlserver import SqlServerConnector
from migration_swamp.request import AcquisitionRequest


def make_connector(req: AcquisitionRequest, spark):
    src = SOURCES[req.source_system]
    if req.source_system == "sql_server":
        return SqlServerConnector(spark)
    if req.source_system == "oracle":
        return OracleConnector(spark, src.endpoint)
    if req.source_system == "sas":
        return SasConnector(spark, src.endpoint, src.max_rows)
    raise ValueError(f"no connector for {req.source_system}")
```

- [ ] **Step 4: Run tests** — `.venv/bin/pytest -q` → all pass (glue classes are imported but not executed).

- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: source connectors with tested pure helpers"`

---

### Task 12: Dev keys + notebooks + README

**Files:**
- Create: `scripts/generate_dev_keys.py`, `keys/README.md`, `notebooks/Acquire_Request.py`, `notebooks/Acquire_Job.py`, `README.md`
- Modify: `migration_swamp/config.py` (append key-loading helpers)

**Interfaces:**
- Consumes: everything above.
- Produces: `config.load_public_key() -> str` (env `SWAMP_PUBLIC_KEY_PEM`, else `keys/dev_public.pem` relative to repo root); committed dev keypair for laptop demos (rotated at work: public key → config/env, private key → secret scope `migration-swamp`, dev keys deleted); the two Databricks source notebooks; project README.

- [ ] **Step 1: Key generation script + config helper + test**

`scripts/generate_dev_keys.py`:
```python
"""Generate the DEV-ONLY keypair committed for laptop demos.
At work: run once, put public key in config/env, private key in the
migration-swamp secret scope, and DELETE the dev keys."""
from pathlib import Path

from migration_swamp.crypto import generate_keypair

keys = Path(__file__).resolve().parent.parent / "keys"
keys.mkdir(exist_ok=True)
private_pem, public_pem = generate_keypair()
(keys / "dev_private.pem").write_text(private_pem)
(keys / "dev_public.pem").write_text(public_pem)
print("wrote keys/dev_private.pem and keys/dev_public.pem (DEV ONLY)")
```

Append to `migration_swamp/config.py`:
```python
import os
from pathlib import Path


def load_public_key() -> str:
    env = os.environ.get("SWAMP_PUBLIC_KEY_PEM")
    if env:
        return env
    dev_key = Path(__file__).resolve().parent.parent / "keys" / "dev_public.pem"
    return dev_key.read_text()
```

Add to `tests/test_config.py`:
```python
def test_load_public_key_env_wins(monkeypatch):
    from migration_swamp.config import load_public_key
    monkeypatch.setenv("SWAMP_PUBLIC_KEY_PEM", "PEM-FROM-ENV")
    assert load_public_key() == "PEM-FROM-ENV"


def test_load_public_key_dev_fallback(monkeypatch):
    from migration_swamp.config import load_public_key
    monkeypatch.delenv("SWAMP_PUBLIC_KEY_PEM", raising=False)
    assert "BEGIN PUBLIC KEY" in load_public_key()
```

- [ ] **Step 2: Run key script, then tests**

Run: `.venv/bin/python scripts/generate_dev_keys.py && .venv/bin/pytest tests/test_config.py -q`
Expected: keys written; tests pass.

`keys/README.md`:
```markdown
# DEV-ONLY keys

`dev_private.pem` / `dev_public.pem` exist so the laptop demo and notebooks
run end-to-end. They protect nothing real. At work: generate a fresh pair,
put the public key in config or `SWAMP_PUBLIC_KEY_PEM`, put the private key
in secret scope `migration-swamp` (key `private_key_pem`), and delete this
directory.
```

- [ ] **Step 3: Write the request notebook** (`notebooks/Acquire_Request.py`, Databricks source format)

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Acquire data — migration-swamp
# MAGIC Pick a source, name the asset, verify your access, and a governed
# MAGIC copy lands in Unity Catalog. You will need your source-system
# MAGIC credentials. Nothing you type is stored.

# COMMAND ----------
import uuid

from migration_swamp import messages
from migration_swamp.config import JOB_NAME, SOURCES, load_public_key
from migration_swamp.connectors.base import Asset
from migration_swamp.connectors.factory import make_connector
from migration_swamp.crypto import encrypt_credentials
from migration_swamp.interactive_auth import prompt_credentials
from migration_swamp.naming import target_path
from migration_swamp.request import (
    AcquisitionRequest, RequestError, to_params, validate,
)

print(messages.WELCOME)

# COMMAND ----------
# Step 1 of 4 — describe the asset you need.
dbutils.widgets.dropdown("source_system", "sql_server", sorted(SOURCES))
dbutils.widgets.text("dbhost", "", "dbhost (SQL Server only)")
dbutils.widgets.text("schema", "", "source schema / SAS libref")
dbutils.widgets.text("table", "", "source table / dataset")
dbutils.widgets.dropdown("gain_access", "yes", ["yes", "no"])
dbutils.widgets.dropdown("refresh", "no", ["yes", "no"])

# COMMAND ----------
# Step 2 of 4 — validate the request.
requester = (spark.sql("SELECT current_user() AS u").first().u)
req = AcquisitionRequest(
    request_id=str(uuid.uuid4()),
    requester=requester,
    source_system=dbutils.widgets.get("source_system"),
    schema=dbutils.widgets.get("schema"),
    table=dbutils.widgets.get("table"),
    dbhost=dbutils.widgets.get("dbhost") or None,
    gain_access=dbutils.widgets.get("gain_access") == "yes",
    refresh=dbutils.widgets.get("refresh") == "yes",
)
try:
    validate(req)
    print(f"Request looks good. Target will be "
          f"{target_path(req.source_system, req.schema, req.table).display}.")
    print("Next: run the cell below to verify your access.")
except RequestError as exc:
    raise SystemExit(f"Please fix the widgets above: {exc}")

# COMMAND ----------
# Step 3 of 4 — verify access (interactive auth; credentials never stored).
creds = prompt_credentials(req.source_system)
asset = Asset(req.source_system, req.schema, req.table, req.dbhost)
display_name = target_path(req.source_system, req.schema, req.table).display
print(messages.probing(display_name))
probe = make_connector(req, spark).probe(asset, creds)
if not probe.ok:
    raise SystemExit(messages.probe_failed(probe.message))
print(messages.probe_ok(display_name))

# COMMAND ----------
# Step 4 of 4 — submit the gated acquisition job (fire and forget).
from databricks.sdk import WorkspaceClient  # DBR built-in

envelope = encrypt_credentials(creds, load_public_key())
del creds  # nothing sensitive stays in notebook state

w = WorkspaceClient()
job = next(w.jobs.list(name=JOB_NAME))
run = w.jobs.run_now(
    job_id=job.job_id,
    job_parameters={**to_params(req), "envelope": envelope},
)
print(f"Submitted request {req.request_id} (run {run.run_id}).")
print(messages.COPY_STARTED)
```

- [ ] **Step 4: Write the job notebook** (`notebooks/Acquire_Job.py`)

```python
# Databricks notebook source
# migration-swamp gated acquisition job. Runs as the service principal.
# Job parameters: the AcquisitionRequest fields plus "envelope".
from datetime import datetime, timezone

from migration_swamp.connectors.factory import make_connector
from migration_swamp.executor import SparkExecutor
from migration_swamp.job import JobDeps, run
from migration_swamp.notify import LoggingNotifier

PARAM_NAMES = ["request_id", "requester", "source_system", "schema",
               "table", "dbhost", "gain_access", "refresh"]
params = {name: dbutils.widgets.get(name) for name in PARAM_NAMES}
envelope = dbutils.widgets.get("envelope")

private_key_pem = dbutils.secrets.get("migration-swamp", "private_key_pem")

deps = JobDeps(
    connector_factory=lambda req: make_connector(req, spark),
    executor=SparkExecutor(spark),
    # Work hardening: swap LoggingNotifier for the approved email pattern.
    notifier=LoggingNotifier(),
    private_key_pem=private_key_pem,
    now=lambda: datetime.now(timezone.utc).isoformat(),
)

result = run(params, envelope, deps)
print(f"status={result.status.value} target={result.target_display} "
      f"rows={result.row_count}")
if result.status.value != "SUCCEEDED":
    raise SystemExit(1)  # mark the run failed in the Jobs UI
```

- [ ] **Step 5: Write `README.md`**

```markdown
# migration-swamp — self-service data acquisition for Databricks

End users prove they can read an on-prem asset (SQL Server, Oracle, SAS)
and a gated job lands a governed shared copy at
`<source_system>.<schema>.<table>` in Unity Catalog, tags it, grants the
requester SELECT, and emails them when it is ready.

Spec: `docs/superpowers/specs/2026-08-14-migration-swamp-design.md`
Plan: `docs/superpowers/plans/2026-08-14-migration-swamp-v1.md`

## Local development

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/pytest
```

The core (validation, naming, crypto, SQL builders, orchestration) is pure
Python and fully tested locally. Spark/JDBC/saspy glue is thin and lazily
imported — it only runs on the work cluster.

## Work hardening checklist (in-tenant)

1. Replace `config.py` endpoints, `AUDIT_TABLE`, and `JOB_NAME` with
   tenant values; confirm target catalogs `sql_server`, `oracle`, `sas`
   exist and the audit catalog/schema is queryable from the serverless
   SQL warehouse.
2. Generate a real keypair (`scripts/generate_dev_keys.py` shows how):
   public key → `SWAMP_PUBLIC_KEY_PEM` or config; private key → secret
   scope `migration-swamp`, key `private_key_pem`. Delete `keys/`.
3. Create the `migration-swamp-acquire` job running
   `notebooks/Acquire_Job.py` as the service principal, on classic jobs
   compute with the approved JDBC drivers and saspy. Grant users
   CAN_MANAGE_RUN (trigger) but not edit.
4. Swap `LoggingNotifier` in `Acquire_Job.py` for the approved company
   email pattern (same `send(to, subject, body)` signature).
5. Align `SqlServerConnector`/`OracleConnector` URL options and the
   `SasConnector` saspy session config with the approved connection
   patterns; confirm the SAS volume guardrail value.
6. Verify end-to-end on one known table per source, then check the
   audit row, tags, and grant from the serverless warehouse.
```

- [ ] **Step 6: Full test run** — `.venv/bin/pytest -q` → all pass.

- [ ] **Step 7: Commit** — `git add -A && git commit -m "feat: notebooks, dev keys, and README"`

---

### Task 13: Final verification sweep

**Files:** none new.

- [ ] **Step 1: Full suite** — `.venv/bin/pytest -q` → all pass, show count.
- [ ] **Step 2: Import check for runtime deps** — Run:
  `.venv/bin/python -c "import migration_swamp.job, migration_swamp.connectors.factory, migration_swamp.notify; print('imports ok')"`
  Expected: `imports ok` (proves no pyspark/saspy needed at import time).
- [ ] **Step 3: Secret-hygiene grep** — Run:
  `grep -rn "password" migration_swamp/ | grep -v -e "creds" -e "password_pem" -e "omrpw" -e '"password"' -e "getpass" -e "never stored"`
  Expected: no line that logs or persists a password (manual eyeball of remaining hits).
- [ ] **Step 4: Spec cross-check** — re-read the spec's Global Constraints against the code (copy-start wording, decision matrix, tag set, dbhost rule, one-audit-row rule). Fix anything off.
- [ ] **Step 5: Commit any fixes** — `git add -A && git commit -m "chore: final verification fixes"` (skip if clean).
