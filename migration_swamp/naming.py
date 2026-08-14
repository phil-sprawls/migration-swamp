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
