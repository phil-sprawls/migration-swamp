"""SAS connector via saspy (lazy import - saspy exists only on the work
cluster). schema maps to the SAS libref, table to the dataset name.
Enforces the volume guardrail from config."""
from migration_swamp.connectors.base import (
    Asset, ConnectorError, ProbeResult,
)
from migration_swamp.crypto import Credentials
from migration_swamp.status import Status


class SasConnector:
    def __init__(self, spark, endpoint: str, max_rows: int | None):
        self._spark = spark
        self._endpoint = endpoint
        self._max_rows = max_rows

    def _session(self, creds: Credentials):
        import saspy  # lazy: work-cluster only

        # Work hardening: replace with the approved saspy config pattern.
        return saspy.SASsession(
            iomhost=self._endpoint, iomport=8591,
            omruser=creds.username, omrpw=creds.password,
        )

    def probe(self, asset: Asset, creds: Credentials) -> ProbeResult:
        try:
            sas = self._session(creds)
            try:
                if not sas.exist(asset.table, libref=asset.schema):
                    return ProbeResult(ok=False,
                                       status=Status.ASSET_NOT_FOUND,
                                       message=f"{asset.schema}.{asset.table} "
                                               "not found")
                return ProbeResult(ok=True)
            finally:
                sas.endsas()
        except Exception as exc:  # noqa: BLE001 - auth/connect failures
            return ProbeResult(ok=False, status=Status.AUTH_FAILED,
                               message=str(exc))

    def read_to_staging(self, asset: Asset, creds: Credentials,
                        staging_view: str) -> int:
        sas = self._session(creds)
        try:
            data = sas.sasdata(asset.table, libref=asset.schema)
            obs = int(data.obs())
            if self._max_rows is not None and obs > self._max_rows:
                raise ConnectorError(
                    Status.VOLUME_EXCEEDED,
                    f"{asset.schema}.{asset.table} has {obs} rows; "
                    f"guardrail is {self._max_rows}")
            pdf = data.to_df()
            df = self._spark.createDataFrame(pdf)
            df.createOrReplaceTempView(staging_view)
            return obs
        except ConnectorError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise ConnectorError(Status.DRIVER_ERROR, str(exc)) from exc
        finally:
            sas.endsas()
