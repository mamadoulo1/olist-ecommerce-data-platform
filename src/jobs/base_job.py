from __future__ import annotations

import time
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession

from src.utils.config import AppConfig
from src.utils.logger import get_logger
from src.utils.spark_utils import get_spark_session

logger = get_logger(__name__)


class BaseJob(ABC):
    """Base class for all pipeline jobs."""

    def __init__(self, config: AppConfig, spark: SparkSession | None = None) -> None:
        self.config = config
        self.spark = spark or get_spark_session(config)

    @abstractmethod
    def run(self) -> None:
        """Execute the job logic."""

    def execute(self) -> None:
        """Wraps run() with start/end logging and elapsed time."""
        job_name = self.__class__.__name__
        logger.info("Job started", extra={"job": job_name})
        start = time.monotonic()
        try:
            self.run()
        finally:
            elapsed = round(time.monotonic() - start, 2)
            logger.info(
                "Job finished", extra={"job": job_name, "elapsed_seconds": elapsed}
            )
