from __future__ import annotations

from src.utils.config import AppConfig


def _load_config() -> AppConfig:
    return AppConfig.load("configs/dev.yaml")


def run_bronze() -> None:
    """Entry point for the Bronze ingestion job (all 8 tables)."""
    from src.jobs.ingestion.bronze_customers_job import BronzeCustomersJob
    from src.jobs.ingestion.bronze_geolocation_job import BronzeGeolocationJob
    from src.jobs.ingestion.bronze_order_items_job import BronzeOrderItemsJob
    from src.jobs.ingestion.bronze_orders_job import BronzeOrdersJob
    from src.jobs.ingestion.bronze_payments_job import BronzePaymentsJob
    from src.jobs.ingestion.bronze_products_job import BronzeProductsJob
    from src.jobs.ingestion.bronze_reviews_job import BronzeReviewsJob
    from src.jobs.ingestion.bronze_sellers_job import BronzeSellersJob

    config = _load_config()
    jobs = [
        BronzeOrdersJob(config),
        BronzeCustomersJob(config),
        BronzeProductsJob(config),
        BronzeSellersJob(config),
        BronzePaymentsJob(config),
        BronzeReviewsJob(config),
        BronzeOrderItemsJob(config),
        BronzeGeolocationJob(config),
    ]
    for job in jobs:
        job.execute()


def run_silver() -> None:
    """Entry point for the Silver transformation job."""
    raise NotImplementedError("Silver layer not yet implemented")


def run_gold_dimensions() -> None:
    """Entry point for the Gold dimensions job."""
    raise NotImplementedError("Gold dimensions not yet implemented")


def run_gold_facts() -> None:
    """Entry point for the Gold facts job."""
    raise NotImplementedError("Gold facts not yet implemented")


def run_uc_bootstrap() -> None:
    """Entry point for Unity Catalog bootstrap."""
    raise NotImplementedError("UC bootstrap not yet implemented")
