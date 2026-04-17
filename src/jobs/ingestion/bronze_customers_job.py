from __future__ import annotations

from src.jobs.ingestion.bronze_base_job import BronzeBaseJob
from src.utils.config import AppConfig

CONTRACT = "olist_customers"


class BronzeCustomersJob(BronzeBaseJob):
    """Ingests olist_customers CSV into the Bronze Delta table."""

    def __init__(self, config: AppConfig, **kwargs) -> None:
        super().__init__(config, contract_name=CONTRACT, **kwargs)

    def _csv_path(self) -> str:
        return f"{self.config.storage.raw_bucket}olist_customers_dataset.csv"
