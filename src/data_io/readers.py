from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from src.utils.logger import get_logger

logger = get_logger(__name__)


class CsvReader:
    """
    Reads a CSV file into a Spark DataFrame using a predefined schema.

    - Applies the schema from SchemaRegistry (no type inference)
    - Corrupted records are routed to the _corrupt_record column
    - Works locally and on S3 (s3://bucket/path/file.csv)
    """

    def __init__(self, spark: SparkSession, schema: StructType):
        self.spark = spark
        self.schema = schema

    def read(self, path: str | Path) -> DataFrame:
        """Reads a CSV file and returns a DataFrame with a _corrupt_record column."""
        from pyspark.sql.types import StringType, StructField, StructType

        # Ajoute une colonne _corrupt_record pour capturer les lignes malformées
        schema_with_corrupt = StructType(
            self.schema.fields + [StructField("_corrupt_record", StringType(), nullable=True)]
        )

        df = (
            self.spark.read.option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
            .schema(schema_with_corrupt)
            .csv(str(path))
        )

        # Cache obligatoire : permet de filtrer _corrupt_record sans relire le fichier
        df.cache()

        logger.info(
            "CSV lu",
            extra={"path": str(path), "schema": self.schema.simpleString()},
        )

        return df


class DeltaReader:
    """
    Reads a Delta table into a Spark DataFrame.

    Usage:
        df = DeltaReader(spark).read("spark_platform_dev.silver.orders")
        df = DeltaReader(spark).read("silver.orders")  # si le catalog est déjà défini
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, table: str) -> DataFrame:
        """Reads a Delta table by its fully qualified name."""
        df = self.spark.read.table(table)

        logger.info("Table Delta lue", extra={"table": table})

        return df


class ParquetReader:
    """
    Reads a Parquet file into a Spark DataFrame.

    Usage:
        df = ParquetReader(spark).read("s3://bucket/path/file.parquet")
        df = ParquetReader(spark).read("/local/path/file.parquet")
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, path: str | Path) -> DataFrame:
        """Reads a Parquet file from the given path."""
        df = self.spark.read.parquet(str(path))

        logger.info("Parquet lu", extra={"path": str(path)})

        return df


class JsonReader:
    """
    Reads a JSON file into a Spark DataFrame.

    Usage:
        df = JsonReader(spark).read("s3://bucket/path/file.json")
        df = JsonReader(spark).read("/local/path/file.json")
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, path: str | Path) -> DataFrame:
        """Reads a JSON file from the given path."""
        df = self.spark.read.json(str(path))

        logger.info("JSON lu", extra={"path": str(path)})

        return df


class DatabaseReader:
    """
    Reads a table from a relational database into a Spark DataFrame using JDBC.

    Usage:
        df = DatabaseReader(spark).read(
            url="jdbc:postgresql://host:port/database",
            table="schema.table",
            properties={"user": "username", "password": "password"}
        )
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read(self, url: str, table: str, properties: dict) -> DataFrame:
        """Reads a table from a relational database using JDBC."""
        df = self.spark.read.jdbc(url=url, table=table, properties=properties)

        logger.info("Table lue depuis la base de données", extra={"url": url, "table": table})

        return df
