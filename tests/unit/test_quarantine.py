import pytest
from pyspark.sql.types import StringType, StructField, StructType

from src.quality.quarantine import QuarantineRouter


@pytest.fixture()
def schema():
    return StructType(
        [
            StructField("order_id", StringType(), nullable=False),
            StructField("order_status", StringType(), nullable=True),
            StructField("_corrupt_record", StringType(), nullable=True),
        ]
    )


@pytest.fixture()
def sample_df(spark, schema):
    data = [
        ("abc123", "delivered", None),  # ligne propre
        (None, "shipped", None),  # order_id manquant → quarantine
        (None, None, "raw,bad,line"),  # ligne malformée → quarantine
    ]
    # PySpark 4.x enforces nullability at createDataFrame time, so we use a
    # fully-nullable schema to insert test NULLs; the router receives the strict schema.
    loose_schema = StructType(
        [StructField(f.name, f.dataType, nullable=True) for f in schema.fields]
    )
    df = spark.createDataFrame(data, loose_schema)
    df.cache()
    return df


class TestQuarantineRouterSplit:
    def test_clean_count(self, sample_df, schema):
        router = QuarantineRouter(schema)
        clean_df, _ = router.split(sample_df)
        assert clean_df.count() == 1

    def test_quarantine_count(self, sample_df, schema):
        router = QuarantineRouter(schema)
        _, quarantine_df = router.split(sample_df)
        assert quarantine_df.count() == 2

    def test_clean_has_no_corrupt_record_col(self, sample_df, schema):
        router = QuarantineRouter(schema)
        clean_df, _ = router.split(sample_df)
        assert "_corrupt_record" not in clean_df.columns

    def test_quarantine_has_reason_col(self, sample_df, schema):
        router = QuarantineRouter(schema)
        _, quarantine_df = router.split(sample_df)
        assert "_quarantine_reason" in quarantine_df.columns

    def test_clean_order_id_is_not_null(self, sample_df, schema):
        from pyspark.sql import functions as F

        router = QuarantineRouter(schema)
        clean_df, _ = router.split(sample_df)
        assert clean_df.filter(F.col("order_id").isNull()).count() == 0


class TestQuarantineReasons:
    def test_malformed_record_reason(self, sample_df, schema):
        router = QuarantineRouter(schema)
        _, quarantine_df = router.split(sample_df)
        malformed = quarantine_df.filter(
            quarantine_df._quarantine_reason == "malformed_record"
        )
        assert malformed.count() == 1

    def test_missing_field_reason(self, sample_df, schema):
        router = QuarantineRouter(schema)
        _, quarantine_df = router.split(sample_df)
        missing = quarantine_df.filter(
            quarantine_df._quarantine_reason == "missing_required_field:order_id"
        )
        assert missing.count() == 1
