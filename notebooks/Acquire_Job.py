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
