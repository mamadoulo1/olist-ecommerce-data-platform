from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import get_logger

logger = get_logger(__name__)


class DeltaWriter:
    """
    Writes DataFrames to Delta tables.

    Four write modes:
    - append             : Bronze — raw data never overwritten
    - merge              : Silver/Gold SCD1 — upsert on natural key
    - merge_scd2         : Gold dim SCD2 — full history tracking
    - overwrite_partition: Gold fact — idempotent partition overwrite
    """

    def __init__(self, table: str):
        self.table = table

    def append(self, df: DataFrame) -> None:
        """Appends data to a Delta table. Creates the table if it doesn't exist."""
        df.write.format("delta").mode("append").saveAsTable(self.table)

        logger.info("Append Delta", extra={"table": self.table, "rows": df.count()})

    def overwrite(self, df: DataFrame) -> None:
        """Overwrites the entire Delta table."""
        df.write.format("delta").mode("overwrite").saveAsTable(self.table)

        logger.info("Overwrite Delta", extra={"table": self.table})

    def overwrite_partition(self, df: DataFrame, partition_col: str) -> None:
        """
        Overwrites only the partitions present in df — leaves other partitions untouched.
        Used for fact_orders partitioned by order_year_month.
        """
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", _build_replace_where(df, partition_col))
            .partitionBy(partition_col)
            .saveAsTable(self.table)
        )

        logger.info(
            "Overwrite partition Delta",
            extra={"table": self.table, "partition_col": partition_col},
        )

    def merge(self, df: DataFrame, natural_key: str) -> None:
        """
        Upserts data into a Delta table on a natural key (SCD Type 1).
        - MATCHED     → UPDATE all columns
        - NOT MATCHED → INSERT new row
        """
        from delta.tables import DeltaTable
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()

        if not _table_exists(spark, self.table):
            # Première exécution — la table n'existe pas encore
            self.overwrite(df)
            return

        delta_table = DeltaTable.forName(spark, self.table)

        update_cols = {c: f"source.{c}" for c in df.columns if c != natural_key}

        (
            delta_table.alias("target")
            .merge(df.alias("source"), f"target.{natural_key} = source.{natural_key}")
            .whenMatchedUpdate(set=update_cols)
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info(
            "Merge Delta (SCD1)",
            extra={"table": self.table, "natural_key": natural_key},
        )

    def merge_scd2(
        self,
        df: DataFrame,
        natural_key: str,
        tracked_cols: list[str],
    ) -> None:
        """
        Applies SCD Type 2 logic on a Delta table.

        - Adds _scd2_hash, valid_from, valid_to, is_current to incoming data
        - Step 1 (MERGE)  : closes changed rows  → valid_to = yesterday, is_current = false
        - Step 2 (APPEND) : inserts new versions  → valid_from = today, valid_to = 9999-12-31
        """
        from delta.tables import DeltaTable
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()

        # Enrichit le source avec les colonnes SCD2
        source = (
            df.withColumn(
                "_scd2_hash",
                F.sha2(F.concat_ws("|", *[F.col(c) for c in tracked_cols]), 256),
            )
            .withColumn("valid_from", F.current_date())
            .withColumn("valid_to", F.lit("9999-12-31").cast("date"))
            .withColumn("is_current", F.lit(True))
        )

        if not _table_exists(spark, self.table):
            source.write.format("delta").mode("overwrite").saveAsTable(self.table)
            logger.info(
                "SCD2 — initialisation table",
                extra={"table": self.table, "natural_key": natural_key},
            )
            return

        delta_table = DeltaTable.forName(spark, self.table)

        # Étape 1 — Ferme les lignes dont le hash a changé
        (
            delta_table.alias("target")
            .merge(
                source.alias("source"),
                f"target.{natural_key} = source.{natural_key}"
                " AND target.is_current = true"
                " AND target._scd2_hash != source._scd2_hash",
            )
            .whenMatchedUpdate(
                set={
                    "valid_to": "date_sub(current_date(), 1)",
                    "is_current": "false",
                }
            )
            .execute()
        )

        # Étape 2 — Insère les nouvelles versions
        # = lignes source sans correspondance courante dans target
        current = (
            spark.read.table(self.table)
            .filter(F.col("is_current") == True)  # noqa: E712
            .select(natural_key)
        )

        to_insert = source.join(current, on=natural_key, how="left_anti")
        rows_to_insert = to_insert.count()

        if rows_to_insert > 0:
            to_insert.write.format("delta").mode("append").saveAsTable(self.table)

        logger.info(
            "Merge Delta (SCD2)",
            extra={
                "table": self.table,
                "natural_key": natural_key,
                "tracked_cols": tracked_cols,
                "rows_inserted": rows_to_insert,
            },
        )


def _table_exists(spark, table: str) -> bool:
    """Checks if a Delta table exists in the catalog."""
    try:
        spark.read.table(table)
        return True
    except Exception:
        return False


def _build_replace_where(df: DataFrame, partition_col: str) -> str:
    """Builds a replaceWhere clause from the distinct partition values in df."""
    values = [row[0] for row in df.select(partition_col).distinct().collect()]
    quoted = ", ".join(f"'{v}'" for v in values)
    return f"{partition_col} IN ({quoted})"
