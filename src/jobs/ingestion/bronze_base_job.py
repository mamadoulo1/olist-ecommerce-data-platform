from __future__ import annotations

from abc import abstractmethod

from src.data_io.readers import CsvReader
from src.data_io.writers import DeltaWriter
from src.jobs.base_job import BaseJob
from src.quality.quarantine import QuarantineRouter
from src.schemas.registry import SchemaRegistry
from src.utils.config import AppConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BronzeBaseJob(BaseJob):
    """Shared ingestion logic for all Bronze CSV jobs."""

    def __init__(self, config: AppConfig, contract_name: str, **kwargs) -> None:
        super().__init__(config, **kwargs)
        self.contract_name = contract_name
        self.schema = SchemaRegistry.get(contract_name)

    def run(self) -> None:
        """Read CSV → quarantine split → append clean rows to Bronze Delta table."""
        csv_path = self._csv_path()
        bronze_table = self._bronze_table()
        quarantine_table = self._quarantine_table()

        logger.info(
            "Bronze ingestion starting",
            extra={
                "contract": self.contract_name,
                "csv_path": csv_path,
                "target_table": bronze_table,
            },
        )

        raw_df = CsvReader(self.spark, self.schema).read(csv_path)

        router = QuarantineRouter(self.schema)
        clean_df, quarantine_df = router.split(raw_df)

        DeltaWriter(bronze_table).append(clean_df)

        if quarantine_df.count() > 0:
            DeltaWriter(quarantine_table).append(quarantine_df)

    @abstractmethod
    def _csv_path(self) -> str:
        """Absolute or relative path to the source CSV file."""

    def _bronze_table(self) -> str:
        return self.config.full_table("bronze", self.contract_name)

    def _quarantine_table(self) -> str:
        return self.config.full_table("bronze", f"{self.contract_name}_quarantine")
