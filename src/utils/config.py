from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class StorageConfig:
    raw_bucket: str        # s3://bucket/raw/olist/
    delta_bucket: str      # s3://bucket/delta/


@dataclass
class SparkConfig:
    master: str            # "local[*]" en dev, "yarn" en prod
    shuffle_partitions: int = 200


@dataclass
class QualityConfig:
    mode: str = "warn"     # "warn" en dev, "fail" en prod


@dataclass
class AppConfig:
    env: str               # "dev" ou "prod"
    catalog: str           # "spark_platform_dev" ou "spark_platform_prod"
    schema_bronze: str = "bronze"
    schema_silver: str = "silver"
    schema_gold: str = "gold"
    storage: StorageConfig = field(default_factory=lambda: StorageConfig("", ""))
    spark: SparkConfig = field(default_factory=SparkConfig)
    quality: QualityConfig = field(default_factory=QualityConfig)

    @classmethod
    def load(cls, path: str | Path) -> AppConfig:
        """Loads config from a YAML file. Path can be overridden by APP_CONFIG env var."""
        config_path = Path(os.getenv("APP_CONFIG", str(path)))

        with open(config_path) as f:
            raw = yaml.safe_load(f)

        return cls(
            env=raw["env"],
            catalog=raw["catalog"],
            schema_bronze=raw.get("schema_bronze", "bronze"),
            schema_silver=raw.get("schema_silver", "silver"),
            schema_gold=raw.get("schema_gold", "gold"),
            storage=StorageConfig(**raw["storage"]),
            spark=SparkConfig(**raw.get("spark", {})),
            quality=QualityConfig(**raw.get("quality", {})),
        )

    def full_table(self, schema: str, table: str) -> str:
        """Returns the fully qualified table name: catalog.schema.table"""
        return f"{self.catalog}.{schema}.{table}"
