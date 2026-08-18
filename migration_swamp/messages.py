from migration_swamp.config import UDAP_INTAKE_URL
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


def probe_failed(reason: str, status: Status | None = None) -> str:
    """Failure text for the notebook. Passing the probe's status appends the
    matching HINT — without it the user only ever sees the raw driver error,
    which for a firewall block says nothing about how to get unblocked."""
    lines = [f"Could not verify access: {reason}"]
    hint = HINTS.get(status, "") if status is not None else ""
    if hint:
        lines.append(hint)
    if status is Status.NETWORK_BLOCKED:
        # Re-running changes nothing until the firewall path is opened.
        lines.append("The flow stops here. Nothing was submitted.")
    else:
        lines.append(
            "The flow stops here. Fix the issue above and re-run this cell.")
    return "\n".join(lines)


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
    Status.NETWORK_BLOCKED: (
        "Databricks cannot reach this SQL Server host. The connection was "
        "refused or timed out before any login was attempted, so this is a "
        "firewall path that has not been opened yet — not a problem with "
        "your credentials or with the table name.\n"
        f"Request SQL Server enablement at {UDAP_INTAKE_URL}. Include the "
        "host and port shown above, the database and schema you need to "
        "read, and your Databricks workspace. Re-run this notebook once "
        "you are notified that the path is open."),
    Status.DRIVER_ERROR: (
        "Unexpected connector error. Contact the platform team with your "
        "request id."),
}
