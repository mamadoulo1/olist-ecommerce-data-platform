from unittest.mock import MagicMock, patch

import pytest

from src.data_io.readers import CsvReader
from src.quality.quarantine import QuarantineRouter
from src.schemas.registry import SchemaRegistry
from src.utils.config import AppConfig

SAMPLE_CSV = "data/olist/sample/olist_orders_dataset.csv"


@pytest.fixture(scope="module")
def orders_schema():
    return SchemaRegistry.get("olist_orders")


@pytest.fixture(scope="module")
def raw_df(spark, orders_schema):
    df = CsvReader(spark, orders_schema).read(SAMPLE_CSV)
    df.cache()
    return df


class TestBronzeOrdersRead:
    def test_reads_sample_csv(self, raw_df):
        assert raw_df.count() > 0

    def test_has_order_id_column(self, raw_df):
        assert "order_id" in raw_df.columns

    def test_has_corrupt_record_column(self, raw_df):
        assert "_corrupt_record" in raw_df.columns

    def test_row_count_matches_sample(self, raw_df):
        assert raw_df.count() == 100


class TestBronzeOrdersQuarantine:
    def test_split_covers_all_rows(self, raw_df, orders_schema):
        router = QuarantineRouter(orders_schema)
        clean_df, quarantine_df = router.split(raw_df)
        assert clean_df.count() + quarantine_df.count() == raw_df.count()

    def test_clean_has_no_nulls_on_order_id(self, raw_df, orders_schema):
        from pyspark.sql import functions as F

        router = QuarantineRouter(orders_schema)
        clean_df, _ = router.split(raw_df)
        assert clean_df.filter(F.col("order_id").isNull()).count() == 0

    def test_clean_has_no_corrupt_record_col(self, raw_df, orders_schema):
        router = QuarantineRouter(orders_schema)
        clean_df, _ = router.split(raw_df)
        assert "_corrupt_record" not in clean_df.columns

    def test_sample_is_mostly_clean(self, raw_df, orders_schema):
        router = QuarantineRouter(orders_schema)
        clean_df, _ = router.split(raw_df)
        assert clean_df.count() >= 90


class TestBronzeOrdersJob:
    def test_job_execute_calls_writer(self, spark):
        """Verifies the job runs end-to-end and calls DeltaWriter.append."""
        config = AppConfig.load("configs/dev.yaml")
        config.storage.raw_bucket = "data/olist/sample/"

        with patch("src.jobs.ingestion.bronze_base_job.DeltaWriter") as mock_writer:
            mock_instance = MagicMock()
            mock_writer.return_value = mock_instance

            from src.jobs.ingestion.bronze_orders_job import BronzeOrdersJob

            job = BronzeOrdersJob(config, spark=spark)
            job.execute()

            assert mock_writer.called
            assert mock_instance.append.called
