# migration-swamp — Self-Service Data Acquisition for Databricks

**Date:** 2026-08-14
**Status:** Approved design (v1)

## Purpose

As the SAS estate sunsets, workloads migrating to Databricks need the data
that SAS extract jobs used to pull. migration-swamp is a self-service data
acquisition system: end users prove they can read an on-prem asset, and the
system lands (or refreshes) a governed shared copy of it in Unity Catalog and
grants them read access.

- **Sources (v1):** SQL Server and Oracle via JDBC, SAS datasets via saspy —
  the connection patterns already proven in the tenant. Lakehouse Federation
  is NOT used (selectively provisioned, not reliably available).
- **Scale target:** hundreds to thousands of tables, end-user initiated.
- **v1 trigger:** manual only. v2 direction: event-driven refresh shortly
  after each on-prem source refreshes (the request→pull decoupling exists for
  exactly this reason).
- **Load semantics (v1):** full replace, idempotent.

## Development model

Built and tested on the personal laptop (pure-Python core, pytest, fake
connectors/executors — no Databricks needed). Then synced to the work laptop
and hardened against company-approved patterns (JDBC/ODBC drivers, saspy
config, secret scopes, job provisioning). Work token budget is spent only on
integration, never on core logic. Runtime code uses only Databricks Runtime
built-in libraries.

## Architecture: three planes with a gate

```
┌─ USER PLANE (request notebook, classic all-purpose compute w/ drivers) ─┐
│ Widgets: action (gain access / refresh data), source system dropdown,   │
│ dbhost (SQL Server only), schema, table.                                │
│ Credentials: interactive auth prompt in the notebook (getpass-style),   │
│ NEVER widgets, never persisted.                                         │
│ Probe the exact asset as the user (SELECT … WHERE 1=0 / saspy open).    │
│ Fail → STOP with a clear message. Pass → envelope-encrypt creds and     │
│ trigger the gated job with (request params, ciphertext).                │
│ If a data copy will run: "Data copy started. You may close this         │
│ notebook. You will receive an email when the data is ready."            │
└──────────────────────────────┬──────────────────────────────────────────┘
                               ▼   gate: job runs as service principal
┌─ PULL PLANE (gated Databricks job, classic jobs compute) ───────────────┐
│ 1. Validate request against policy                                      │
│ 2. Decrypt creds (private key in job-only secret scope)                 │
│ 3. Re-probe the asset as the user — the authoritative entitlement check │
│ 4. Decide work:                                                         │
│      copy missing             → full pull (even if user chose access-only)│
│      refresh selected         → full-replace pull                        │
│      copy exists, access-only → skip pull                                │
│ 5. Write shared copy at fixed path <source_system>.<schema>.<table>     │
│    (e.g. sql_server.prod_db.data_table), owned by the service principal │
│ 6. Apply tags (source_system=…, acquisition_type=copy)                  │
│ 7. GRANT SELECT to requester; write audit row; discard creds            │
│ 8. Email the requester: success (table ready, path, row count) or       │
│    failure (classified reason + remediation hint)                       │
└──────────────────────────────┬──────────────────────────────────────────┘
                               ▼
┌─ CONSUMPTION PLANE (pure SQL, serverless SQL warehouse) ────────────────┐
│ Shared governed copies + acquisition_log: users browse what exists,     │
│ check run status, and query data — all plain SQL.                       │
└─────────────────────────────────────────────────────────────────────────┘
```

Key properties:

- **The user never executes the pull.** Naming, tagging, ownership, and
  grants cannot be bypassed because only the gated job writes tables.
- **Entitlement = ability to read the on-prem asset.** Proven twice: in the
  notebook (fast feedback) and again inside the job (authoritative — the
  grant is never based on an unverifiable claim from the notebook).
- **One shared copy per source asset** at a deterministic UC path. A second
  user "gaining access" to an already-copied table gets a GRANT, not a new
  copy.
- **Everything user-facing after the trigger is pure SQL** on the serverless
  warehouse.

## Naming, tagging, access

- **Path:** catalog = source system name (`sql_server`, `oracle`, `sas`);
  schema and table mirror the source, sanitized to valid UC identifiers
  (lowercase, non-alphanumerics → `_`, collision-safe). The same asset always
  writes to the same location.
- **Tags** (UC table tags): `source_system`, `acquisition_type=copy`, plus
  audit properties: source host (SQL Server), source schema/table,
  `acquired_by`, `acquired_at`, row count.
- **Ownership:** tables owned by the job's service principal. Requester gets
  `GRANT SELECT`. (Per-user ownership was considered and rejected — shared
  copies must survive any individual user.)
- **Access model:** proving on-prem read access to the asset entitles the
  user to read the cloud copy. Probe granularity is table-level
  (`SELECT ... WHERE 1=0` / saspy dataset open), matching the granularity of
  the grant.

## Components (repo layout)

```
migration-swamp/
├── pyproject.toml, README.md
├── migration_swamp/                  # package — DBR built-ins only
│   ├── config.py        # source registry: oracle/sas fixed endpoints;
│   │                    #   sql_server requires dbhost; target catalogs;
│   │                    #   guardrails (SAS volume limit, timeouts);
│   │                    #   RSA public key
│   ├── request.py       # AcquisitionRequest dataclass + validation
│   │                    #   (actions, source, schema, table, dbhost rule)
│   ├── naming.py        # derive & sanitize <source_system>.<schema>.<table>
│   ├── crypto.py        # envelope encryption (AES-GCM data key wrapped
│   │                    #   with RSA-OAEP); encrypt in notebook, decrypt in
│   │                    #   job from secret-scope private key
│   ├── interactive_auth.py  # getpass-style credential prompt; the single
│   │                    #   swap point for the company auth pattern
│   ├── connectors/
│   │   ├── base.py      # interface: probe(asset) -> ProbeResult,
│   │   │                #   read_full(asset) -> DataFrame
│   │   ├── sqlserver.py # JDBC (host from request)
│   │   ├── oracle.py    # JDBC (endpoint from config)
│   │   └── sas.py       # saspy; enforces volume guardrail
│   ├── governance.py    # SQL builders: CREATE OR REPLACE TABLE AS,
│   │                    #   SET TAGS, GRANT SELECT
│   ├── audit.py         # acquisition_log row construction & write
│   ├── notify.py        # Notifier interface + email composition; laptop
│   │                    #   impl logs/SMTP-stubs, work impl uses the
│   │                    #   approved company email pattern
│   ├── messages.py      # user-facing step/status/next-step messages shared
│   │                    #   by notebook and job output
│   └── job.py           # pull-plane orchestration: validate → decrypt →
│                        #   re-probe → decide → pull → write → tag →
│                        #   grant → audit; creds never logged
├── notebooks/
│   ├── Acquire_Request.py   # user plane (widgets + interactive auth +
│   │                        #   probe + trigger + poll/notify)
│   └── Acquire_Job.py       # job entry point → migration_swamp.job.run()
└── tests/                   # pytest; fake connectors + fake SQL executor
```

Connectors are swappable and individually testable; `job.py` knows nothing
about JDBC vs saspy. `messages.py` centralizes wording so the notebook
narration and job status stay consistent.

## User experience (request notebook)

The notebook narrates every step so the user always knows what is happening
and what comes next:

1. **Welcome / instructions** — what the tool does, what they'll need.
2. **Inputs** — widgets: action checkboxes (gain access / refresh data),
   source system dropdown, dbhost (required only for SQL Server),
   schema, table.
3. **Interactive auth** — prompt for source credentials with an explanation
   of why (entitlement proof) and assurance they are never stored.
4. **Probe** — "Verifying you can read sql_server.prod_db.data_table…" with
   a clear success or actionable failure message (flow stops on failure).
5. **Submit** — "Submitting gated acquisition job (run #…)". Ciphertext-only
   handoff explained in one line.
6. **Hand off & notify** — fire-and-forget:
   - If a data copy will run (copy missing or refresh selected), the
     notebook prints: "Data copy started. You may close this notebook.
     You will receive an email when the data is ready." The job sends the
     email on completion — success (table path, row count, how to query on
     the serverless warehouse) or failure (classified reason + remediation
     hint). The user does not need to keep the notebook open.
   - If no copy is needed (grant-only on an existing table), the job is
     typically fast; the user is told access is being granted and receives
     the same completion email.
   - acquisition_log remains queryable for status at any time.

## Credential handling

- Collected only via interactive prompt in the notebook
  (`interactive_auth.py`); never widgets, never written to disk or state.
- Envelope encryption: fresh AES-GCM data key per request encrypts
  `{username, password}`; the data key is wrapped with the system RSA public
  key (in config). Only ciphertext travels as a job parameter, so run
  history is safe.
- The job's service principal reads the RSA private key from secret scope
  `migration-swamp`, decrypts in memory, uses the creds for re-probe + pull,
  then discards them.
- Rotation: generate a new keypair, update the secret and the config public
  key.
- A scrubbing wrapper around exceptions guarantees credentials never appear
  in logs, error messages, or audit rows.

## Error handling & audit

Every job run writes exactly one row to `acquisition_log` (Delta, queryable
from the serverless warehouse): request id, requester, action, source
coordinates, target path, classified status, timings, row count, and a
remediation hint. Statuses:

`SUCCEEDED · AUTH_FAILED · ASSET_NOT_FOUND · VOLUME_EXCEEDED (SAS guardrail)
· POLICY_REJECTED · DRIVER_ERROR`

Full-replace writes are idempotent. Concurrent requests for the same asset
are last-writer-wins in v1 (acceptable: identical full pulls); audit rows are
deduped by request id.

## Testing strategy

All on the laptop, pytest, no Databricks required for the core:

- naming derivation and sanitization (including collision and edge cases)
- request validation (dbhost required iff SQL Server; action combinations)
- crypto round-trip and tamper rejection
- work-decision matrix: copy-missing × refresh × access-only
- governance SQL builders (exact statements asserted)
- audit row construction and status classification
- `job.py` orchestration end-to-end against fake connectors and a fake SQL
  executor that records every issued statement
- credential-scrubbing wrapper (no secret material in any raised error)
- notification composition (success/failure email content) against a fake
  Notifier

The JDBC/saspy glue is deliberately thin and is hardened at work against the
approved connection patterns.

## Out of scope (v1)

- Event-driven / scheduled refresh (v2 — the pull plane is re-drivable by a
  scheduler without changes to the request contract)
- Incremental / CDC loads (full replace only)
- Request-approval workflows (entitlement proof is the gate)
- A dedicated UI (notebook is the v1 surface)
- Data classification / PII tagging (naming + lineage tags only in v1)
