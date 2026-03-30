from src.data_io.readers import CsvReader
from src.schemas.registry import SchemaRegistry
from src.utils.config import AppConfig
from src.utils.spark_utils import get_spark_session

config = AppConfig.load("configs/dev.yaml")
spark = get_spark_session(config)

schema = SchemaRegistry.get("olist_orders")
reader = CsvReader(spark, schema)

df = reader.read("data/olist/olist_orders_dataset.csv")

print("=== Schéma lu ===")
df.printSchema()

print(f"\n=== Nombre de lignes : {df.count()} ===")

print("\n=== 3 premières lignes ===")
df.show(3, truncate=False)

print("\n=== Lignes corrompues ===")
corrupted = df.filter(df._corrupt_record.isNotNull())
print(f"Nombre : {corrupted.count()}")

spark.stop()
