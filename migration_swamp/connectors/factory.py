from migration_swamp.config import SOURCES
from migration_swamp.connectors.oracle import OracleConnector
from migration_swamp.connectors.sas import SasConnector
from migration_swamp.connectors.sqlserver import SqlServerConnector
from migration_swamp.request import AcquisitionRequest


def make_connector(req: AcquisitionRequest, spark):
    src = SOURCES[req.source_system]
    if req.source_system == "sql_server":
        return SqlServerConnector(spark)
    if req.source_system == "oracle":
        return OracleConnector(spark, src.endpoint)
    if req.source_system == "sas":
        return SasConnector(spark, src.endpoint, src.max_rows)
    raise ValueError(f"no connector for {req.source_system}")
