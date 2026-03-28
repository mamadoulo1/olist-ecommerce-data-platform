from src.utils.config import AppConfig
from src.utils.spark_utils import get_spark_session

# Charger la config dev
config = AppConfig.load("configs/dev.yaml")

# Créer la SparkSession
spark = get_spark_session(config, app_name="test-spark")

# Test basique — créer un DataFrame simple
data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

df.show()
print("Version Spark :", spark.version)
print("Shuffle partitions :", spark.conf.get("spark.sql.shuffle.partitions"))

spark.stop()
