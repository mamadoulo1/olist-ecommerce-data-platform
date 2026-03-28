from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

from src.utils.config import AppConfig
from src.utils.logger import get_logger

logger = get_logger(__name__)


def get_spark_session(config: AppConfig, app_name: str = "olist-platform") -> SparkSession:
    """
    Creates or retrieves a SparkSession configured for the given environment.

    - Local (dev)  : session locale avec Delta Lake via pip JARs
    - Databricks   : session existante récupérée, Unity Catalog configuré
    """
    active = SparkSession.getActiveSession()

    if active is not None:
        # Session existante → on est sur Databricks (dev ou prod)
        spark = active
        _configure_unity_catalog(spark, config)
    else:
        # Pas de session → on est en local sur sa machine
        spark = _build_local_session(config, app_name)

    logger.info(
        "SparkSession prête",
        extra={
            "app_name": app_name,
            "env": config.env,
            "master": config.spark.master,
            "shuffle_partitions": config.spark.shuffle_partitions,
        },
    )

    return spark


def _build_local_session(config: AppConfig, app_name: str) -> SparkSession:
    """Builds a SparkSession for local dev with Delta Lake via pip JARs."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(config.spark.master)
        .config("spark.sql.shuffle.partitions", str(config.spark.shuffle_partitions))
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()


def _configure_unity_catalog(spark: SparkSession, config: AppConfig) -> None:
    """Sets the default catalog to the one defined in config (Unity Catalog)."""
    spark.sql(f"USE CATALOG {config.catalog}")
    logger.info("Unity Catalog configuré", extra={"catalog": config.catalog})
