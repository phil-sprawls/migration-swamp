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
