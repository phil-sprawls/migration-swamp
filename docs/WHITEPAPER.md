# migration-swamp — a whitepaper

**Status:** draft code, not yet deployed. Written for the architects and
engineers who will evaluate this design, finish the in-tenant work, and
launch it.

---

## 1. The problem

Analysts need on-prem data (SQL Server, Oracle, SAS) in Databricks. Today
that means filing a ticket and waiting for a platform engineer to hand-write
a pull. The work is repetitive, the copies land wherever the engineer put
them, and nobody can answer "where did this table come from?" six months
later.

migration-swamp makes it self-service **without** making it ungoverned. An
analyst describes the asset they need, proves they can already read it on
the source system, and a gated job lands a governed copy in Unity Catalog —
named consistently, tagged with its provenance, granted only to them, and
written to an audit log.

The guiding constraint: **self-service must not become a privilege
escalation path.** A user must not be able to obtain data through this tool
that they could not already read at the source.

---

## 2. How it works

```mermaid
sequenceDiagram
    participant U as Analyst<br/>(Acquire_Request notebook)
    participant S as Source system<br/>(on-prem)
    participant J as Gated job<br/>(service principal)
    participant UC as Unity Catalog

    U->>U: 1. Describe asset (widgets) → validate
    U->>U: 2. Enter source credentials (never stored)
    U->>S: 3. Probe: SELECT * WHERE 1=0
    S-->>U: entitlement confirmed
    U->>U: 4. Encrypt credentials to job's public key
    U->>J: 5. run_now(request params + envelope)
    Note over U: notebook exits; user closes it
    J->>J: 6. Decrypt envelope (private key from secret scope)
    J->>S: 7. Re-probe, then full read → staging view
    J->>UC: 8. CREATE TABLE, SET TAGS, GRANT SELECT
    J->>UC: 9. Append audit row
    J-->>U: 10. Email: your data is ready
```

### The four steps the analyst sees

1. **Describe the asset.** Source system, schema, table, and for SQL Server
   the host. Plus two switches: `gain_access` and `refresh`.
2. **Validate.** Pure-Python checks in `request.py`; the target name is
   computed and shown up front so there is no surprise about where the data
   lands.
3. **Verify access.** The notebook prompts for source credentials and runs a
   zero-row probe (`SELECT * FROM schema.table WHERE 1=0`). This is the
   entitlement check — cheap, reads no data, and proves the user personally
   can read the asset. **If the probe fails, the flow stops and nothing is
   submitted.**
4. **Submit.** Credentials are encrypted, the job is triggered, and the
   notebook exits. The user closes it and waits for an email.

---

## 3. Three design decisions worth reviewing

These are the parts an architecture review should focus on.

### 3.1 Probe-before-submit is the authorization model

There is no approval queue and no entitlement database. The user's own
ability to read the asset **at the source, with their own credentials** is
the authorization. The tool copies data on behalf of someone who could
already read it, so the copy grants no new access. This is why the probe is
non-negotiable and why the job re-probes rather than trusting the notebook's
result.

### 3.2 Credentials are envelope-encrypted and bound to the request

The user's source credentials must reach the job, but must not be readable
from job parameters, run history, or logs.

`crypto.py` implements RSA-3072/OAEP-SHA256 + AES-256-GCM envelope
encryption. The notebook encrypts to a **public** key; only the job, holding
the private key from a Databricks secret scope, can decrypt.

The important detail is the **AAD binding**: the request's canonical fields
are passed as AES-GCM additional authenticated data (`canonical_aad`). An
envelope captured from one request cannot be replayed against a different
request — swap the table name and decryption fails outright. Credentials are
deleted from notebook state immediately after encryption, and `scrub.py`
strips them from every audit row and email.

### 3.3 The job is the only privileged actor

The notebook runs as the user and can only *trigger* the job. The job runs
as a **service principal** and is the sole writer to Unity Catalog. Users
get `CAN_MANAGE_RUN` on the job — never edit rights.

**This is a real security boundary, not a convention.** The job trusts its
own parameters, including `requester`. If a user could edit the job, they
could alter its logic or grant a table to someone else. Job ACLs must be
verified at deploy time and treated as part of the threat model.

---

## 4. What lands in Unity Catalog

| Concern | Behavior |
|---|---|
| Naming | `<source_system>.<schema>.<table>`, each part sanitized to `[a-z0-9_]` (`naming.py`). Deterministic — the same asset always maps to the same target. |
| Tags | `source_system`, `source_schema`, `source_table`, `source_host`, `acquired_by`, `acquired_at`, `row_count`, `acquisition_type` (`governance.py`). Provenance is queryable. |
| Grants | `GRANT SELECT` to the requester only. No blanket grants; identifiers are backtick-escaped. |
| Audit | One row per run attempt in `AUDIT_TABLE`: request, target, status, row count, timings, scrubbed hint. |
| Refresh | An existing copy is reused unless `refresh=yes`, which does `CREATE OR REPLACE`. |

**Audit semantics to note:** rows are per *run attempt*, not per request. A
retried run writes a second row for the same `request_id`. Either dedupe
downstream (latest `finished_at` wins) or disable retries on the job.

---

## 5. Failure handling, and the SQL Server firewall path

Every failure resolves to a `Status` (`status.py`), and every status carries
a plain-language hint (`messages.HINTS`). The same hint text is shown in the
notebook and sent in the failure email, so the user never sees a bare Java
stack trace.

| Status | Meaning |
|---|---|
| `AUTH_FAILED` | Credentials rejected by the source. |
| `ASSET_NOT_FOUND` | Schema/table does not exist. |
| `VOLUME_EXCEEDED` | SAS dataset over the transfer guardrail. |
| `POLICY_REJECTED` | Validation failed, or the credential envelope was unreadable. |
| `NETWORK_BLOCKED` | **SQL Server host not reachable — firewall path not open.** |
| `DRIVER_ERROR` | Anything else. |

### Why `NETWORK_BLOCKED` exists

Oracle and SAS use fixed endpoints that are already enabled tenant-wide.
**SQL Server is different: each host must be opened to Databricks
individually.** A user's first attempt against a new server is therefore the
single most likely failure in the whole flow — and its raw symptom is
useless, because a dropped packet surfaces as a driver socket timeout that
looks identical to a hung server.

Two mechanisms turn that into an actionable instruction:

1. **TCP preflight** (`tcp_preflight`) — before spending a login timeout,
   the connector opens a socket to the host and port with a 5-second budget.
   A dropped packet would otherwise hang the JDBC login for 30 seconds or
   more with no explanation.
2. **Error classification** (`classify_sqlserver_error`) — a backstop for
   blocks the socket check cannot see, matching driver-level socket
   signatures.

Either path produces `NETWORK_BLOCKED`, and the user is told the connection
was refused *before any login was attempted* — so it is not their password
and not their table name — and is directed to request SQL Server enablement
at **`go/udapintake`**, with the host, port, database, schema, and workspace
to include in the request.

Two deliberate limits, both worth a reviewer's attention:

- The preflight returns **undetermined** (and defers to the driver) for a
  named instance like `host\PROD` with no explicit port, because SQL Browser
  assigns those dynamically — probing 1433 would report a block that isn't
  real.
- `classify_jdbc_error`, which the Oracle connector shares, has **no**
  firewall branch. Oracle's path is already enabled, so an Oracle socket
  failure is a genuine fault and must not send those users to the SQL Server
  intake form.

---

## 6. Code map

```
migration_swamp/
  request.py        AcquisitionRequest, validation, canonical AAD, params
  crypto.py         envelope encryption/decryption (RSA + AES-GCM)
  naming.py         identifier sanitizing → TargetPath
  governance.py     CREATE TABLE / SET TAGS / GRANT SELECT builders
  job.py            orchestration: decrypt → probe → read → govern → audit → notify
  executor.py       SqlExecutor seam (SparkExecutor at runtime, fake in tests)
  audit.py          audit row + INSERT builders
  notify.py         email composition; Notifier seam
  messages.py       all user-facing text and hints
  scrub.py          removes secrets from anything user-visible
  status.py         the Status enum
  config.py         tenant values: endpoints, AUDIT_TABLE, JOB_NAME, intake URL
  connectors/       sqlserver.py, oracle.py, sas.py + factory
notebooks/
  Acquire_Request.py   what the analyst runs
  Acquire_Job.py       what the service principal runs
```

**Testability is the reason for the shape.** Validation, naming, crypto, SQL
building, classification, and orchestration are pure Python behind injected
seams (`SqlExecutor`, `Notifier`, `connector_factory`, and the socket opener
in `tcp_preflight`). The suite runs on a laptop with no cluster:

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/pytest        # 70 tests
```

Spark/JDBC/saspy glue is thin and lazily imported — it only executes on the
work cluster, and it is the part that has **never been run**.

---

## 7. What is not done — the completion work

This is draft code. Everything below is required before launch and is
tracked in the README's hardening checklist.

| # | Work | Risk if skipped |
|---|---|---|
| 1 | Replace `config.py` values: source endpoints, `AUDIT_TABLE`, `JOB_NAME`, and confirm `UDAP_INTAKE_URL` is the correct intake link | Users sent to a dead link; job cannot find its target |
| 2 | Generate a real keypair; private key → secret scope `migration-swamp`, public key → config/env. **Delete `keys/`** (dev keys are in the repo) | Anyone with repo access could decrypt credentials |
| 3 | Create the `migration-swamp-acquire` job on classic compute with approved JDBC drivers + saspy, running as the service principal | — |
| 4 | Grant users `CAN_MANAGE_RUN` only; verify run-history parameter visibility does not leak other users' requests | Privilege escalation (see §3.3) |
| 5 | Swap `LoggingNotifier` for the approved email pattern (same `send(to, subject, body)` signature) | Users are never told their data is ready |
| 6 | Align JDBC URL options and the saspy session config with approved connection patterns; confirm the SAS volume guardrail | Connections rejected by source standards |
| 7 | Confirm target catalogs `sql_server`, `oracle`, `sas` exist and the audit table is queryable from the serverless warehouse | Job fails at the governance step |
| 8 | End-to-end test on one known table per source; verify audit row, tags, and grant | — |

**Unvalidated assumption to test first:** the TCP preflight assumes the
driver node can open outbound sockets directly. If egress is via proxy or a
Private Link path where a raw socket behaves differently from the JDBC
connection, the preflight could report a false block. Verify against one
enabled host and one deliberately non-enabled host before launch; if it
misreports, the classifier backstop alone still delivers the message.

---

## 8. Open questions for the review

1. **Full-table copies only.** No incremental loads, no partitioning, no
   predicate pushdown. Is that acceptable for the expected table sizes, and
   should SQL Server and Oracle get a volume guardrail like SAS has?
2. **No cost ceiling.** A user can trigger a large pull unattended. Should
   there be a row/byte limit or a cluster policy cap?
3. **Copies are static.** A copy never refreshes unless someone re-runs with
   `refresh=yes`. Who owns staleness, and should the tags drive a monitor?
4. **Requester-only grants.** Copies are shared objects but granted to one
   person. Is group-based granting needed, and who de-provisions on leavers?
5. **Audit rows are per-attempt.** Accept and dedupe downstream, or disable
   job retries? (§4)
6. **Named-instance SQL Server hosts** skip the preflight. If those are
   common, is discovering the port via SQL Browser worth the added
   complexity?

---

## 9. Recommended launch sequence

1. Architecture review of §3 (trust model) and §8 (open questions).
2. Security review of the credential path: envelope + AAD binding, secret
   scope, job ACLs, scrubbing.
3. Complete §7 items 1–7 in a non-production workspace.
4. End-to-end test per source, including one deliberately blocked SQL Server
   host to confirm the `go/udapintake` message appears.
5. Pilot with a small analyst group; watch the audit table for the real
   failure-status distribution.
6. Launch with a documented intake path for users whose source is not yet
   enabled.

---

*Design detail: `docs/superpowers/specs/2026-08-14-migration-swamp-design.md`
· Build plan: `docs/superpowers/plans/2026-08-14-migration-swamp-v1.md`*
