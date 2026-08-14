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
    safe_principal = principal.replace("`", "``")
    return f"GRANT SELECT ON TABLE {target.fqn} TO `{safe_principal}`"
