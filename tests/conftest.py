import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1]))


@pytest.fixture(scope="session")
def spark():
    """SparkSession locale partagée par tous les tests de la session pytest."""
    from src.utils.config import AppConfig
    from src.utils.spark_utils import get_spark_session

    config = AppConfig.load("configs/dev.yaml")
    session = get_spark_session(config, app_name="olist-tests")
    yield session
    session.stop()
