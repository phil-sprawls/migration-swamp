# migration-swamp — self-service data acquisition for Databricks

End users prove they can read an on-prem asset (SQL Server, Oracle, SAS)
and a gated job lands a governed shared copy at
`<source_system>.<schema>.<table>` in Unity Catalog, tags it, grants the
requester SELECT, and emails them when it is ready.

**Start here for review:** `docs/WHITEPAPER.md` — how it works, the trust
model, what is still unfinished, and the launch sequence.

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
   SQL warehouse. Confirm `UDAP_INTAKE_URL` (`go/udapintake`) is the
   correct SQL Server enablement intake link and that
   `PREFLIGHT_TIMEOUT_SECONDS` suits in-tenant network latency.
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
7. Audit rows are per-run-attempt, not per request: a retried run writes
   another row for the same `request_id`. Dedupe by `request_id` downstream
   (keep latest `finished_at`) or disable job retries on
   `migration-swamp-acquire`.
8. Verify the SQL Server TCP preflight against one enabled host and one
   deliberately non-enabled host. It assumes the driver node can open
   outbound sockets directly; if egress is proxied or via Private Link, a
   raw socket may behave differently from the JDBC connection and report a
   false `NETWORK_BLOCKED`. The `classify_sqlserver_error` backstop still
   delivers the `go/udapintake` message if the preflight is unreliable.
9. The job trusts its own parameters — job ACLs are part of the security
   boundary, not a formality. Grant users CAN_MANAGE_RUN only (no edit) so
   they cannot alter job logic or override the service-principal identity,
   and verify that Databricks run-history parameter visibility in this
   tenant doesn't leak other users' request details across principals.
