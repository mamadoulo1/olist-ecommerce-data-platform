from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Dossier contracts/ à la racine du projet
_CONTRACTS_DIR = Path(__file__).parents[2] / "contracts"

# Correspondance type JSON → type Spark
_TYPE_MAP = {
    "string": StringType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "timestamp": TimestampType(),
    "boolean": BooleanType(),
}


class SchemaRegistry:
    """
    Lit les contrats JSON et retourne le StructType Spark correspondant.

    Usage:
        schema = SchemaRegistry.get("olist_orders")
        df = spark.read.csv(path, schema=schema, header=True)
    """

    _cache: dict[str, StructType] = {}

    @classmethod
    def get(cls, name: str) -> StructType:
        """
        Returns the Spark StructType for a given contract name.

        Args:
            name: nom du contrat sans version ni extension (ex: "olist_orders")
        """
        if name in cls._cache:
            return cls._cache[name]

        contract_path = cls._find_contract(name)
        schema = cls._parse(contract_path)
        cls._cache[name] = schema
        return schema

    @classmethod
    def _find_contract(cls, name: str) -> Path:
        """Finds the latest version of a contract by name."""
        matches = sorted(_CONTRACTS_DIR.glob(f"{name}_v*.json"))

        if not matches:
            raise FileNotFoundError(
                f"Aucun contrat trouvé pour '{name}' dans {_CONTRACTS_DIR}"
            )

        # Prend la version la plus récente (tri alphabétique → v1 < v2 < v9)
        return matches[-1]

    @classmethod
    def _parse(cls, path: Path) -> StructType:
        """Parses a JSON contract file into a Spark StructType."""
        with open(path) as f:
            contract = json.load(f)

        fields = []
        for field in contract["fields"]:
            spark_type = _TYPE_MAP.get(field["type"])

            if spark_type is None:
                raise ValueError(
                    f"Type inconnu '{field['type']}' dans le contrat {path.name}"
                )

            fields.append(
                StructField(
                    name=field["name"],
                    dataType=spark_type,
                    nullable=field["nullable"],
                    metadata={"description": field.get("description", "")},
                )
            )

        return StructType(fields)
