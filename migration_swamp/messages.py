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
