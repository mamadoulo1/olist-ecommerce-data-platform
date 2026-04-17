from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.utils.logger import get_logger

logger = get_logger(__name__)


class QuarantineRouter:
    """
    Splits a DataFrame into clean rows and corrupted rows.

    A row is quarantined if:
    - _corrupt_record is not null  (malformed CSV line)
    - a non-nullable column is null (missing required field)
    """

    def __init__(self, schema: StructType):
        self.schema = schema
        self._required_cols = [f.name for f in schema.fields if not f.nullable]

    def split(self, df: DataFrame) -> tuple[DataFrame, DataFrame]:
        """
        Returns (clean_df, quarantine_df).

        clean_df      : rows with no corruption and all required fields present
        quarantine_df : rows with _corrupt_record set OR missing required fields
        """
        corrupt_condition = self._build_corrupt_condition(df)

        quarantine_df = df.filter(corrupt_condition).withColumn(
            "_quarantine_reason", self._build_reason_col(df)
        )

        clean_df = df.filter(~corrupt_condition).drop("_corrupt_record")

        clean_count = clean_df.count()
        quarantine_count = quarantine_df.count()

        logger.info(
            "Quarantine split",
            extra={
                "clean_rows": clean_count,
                "quarantine_rows": quarantine_count,
                "required_cols_checked": self._required_cols,
            },
        )

        return clean_df, quarantine_df

    def _build_corrupt_condition(self, df: DataFrame):
        """Condition = ligne malformée OU champ obligatoire null."""
        condition = F.col("_corrupt_record").isNotNull()

        for col_name in self._required_cols:
            if col_name in df.columns:
                condition = condition | F.col(col_name).isNull()

        return condition

    def _build_reason_col(self, df: DataFrame):
        """Builds a human-readable reason string for each quarantined row."""
        reason = F.when(F.col("_corrupt_record").isNotNull(), F.lit("malformed_record"))

        for col_name in self._required_cols:
            if col_name in df.columns:
                reason = reason.when(
                    F.col(col_name).isNull(),
                    F.lit(f"missing_required_field:{col_name}"),
                )

        return reason.otherwise(F.lit("unknown"))
