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
