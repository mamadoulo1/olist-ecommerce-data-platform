import pytest
from pyspark.sql.types import (
    StringType,
    StructType,
    TimestampType,
)

from src.schemas.registry import SchemaRegistry


@pytest.fixture(autouse=True)
def clear_cache():
    """Vide le cache avant chaque test pour garantir l'isolation."""
    SchemaRegistry._cache.clear()
    yield
    SchemaRegistry._cache.clear()


class TestSchemaRegistryGet:
    def test_returns_struct_type(self):
        schema = SchemaRegistry.get("olist_orders")
        assert isinstance(schema, StructType)

    def test_orders_has_expected_columns(self):
        schema = SchemaRegistry.get("olist_orders")
        field_names = [f.name for f in schema.fields]

        assert "order_id" in field_names
        assert "customer_id" in field_names
        assert "order_status" in field_names
        assert "purchase_ts" in field_names

    def test_orders_types_are_correct(self):
        schema = SchemaRegistry.get("olist_orders")
        fields = {f.name: f for f in schema.fields}

        assert isinstance(fields["order_id"].dataType, StringType)
        assert isinstance(fields["purchase_ts"].dataType, TimestampType)

    def test_nullable_constraints(self):
        schema = SchemaRegistry.get("olist_orders")
        fields = {f.name: f for f in schema.fields}

        # order_id est obligatoire
        assert fields["order_id"].nullable is False

        # approved_ts peut être null (commande annulée)
        assert fields["approved_ts"].nullable is True

    def test_unknown_contract_raises_error(self):
        with pytest.raises(FileNotFoundError, match="table_inexistante"):
            SchemaRegistry.get("table_inexistante")


class TestSchemaRegistryCache:
    def test_cache_returns_same_object(self):
        schema1 = SchemaRegistry.get("olist_orders")
        schema2 = SchemaRegistry.get("olist_orders")
        assert schema1 is schema2

    def test_cache_is_populated_after_first_call(self):
        assert "olist_orders" not in SchemaRegistry._cache
        SchemaRegistry.get("olist_orders")
        assert "olist_orders" in SchemaRegistry._cache


class TestSchemaRegistryAllContracts:
    """Vérifie que les 8 contrats sont lisibles sans erreur."""

    @pytest.mark.parametrize(
        "name",
        [
            "olist_orders",
            "olist_order_items",
            "olist_customers",
            "olist_products",
            "olist_sellers",
            "olist_payments",
            "olist_reviews",
            "olist_geolocation",
        ],
    )
    def test_all_contracts_load(self, name):
        schema = SchemaRegistry.get(name)
        assert isinstance(schema, StructType)
        assert len(schema.fields) > 0
