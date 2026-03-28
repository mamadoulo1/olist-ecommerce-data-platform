# from pathlib import Path
# import yaml
# from src.utils.config import AppConfig, StorageConfig, SparkConfig, QualityConfig

# # --- Test 1 : créer un AppConfig manuellement (sans YAML)
# config = AppConfig(
#     env="dev",
#     catalog="spark_platform_dev",
#     storage=StorageConfig(
#         raw_bucket="s3://platform-dev/raw/",
#         delta_bucket="s3://platform-dev/delta/",
#     ),
#     spark=SparkConfig(master="local[*]", shuffle_partitions=8),
#     quality=QualityConfig(mode="warn"),
# )

# print("=== Test 1 : config manuelle ===")
# print(f"env        : {config.env}")
# print(f"catalog    : {config.catalog}")
# print(f"raw_bucket : {config.storage.raw_bucket}")
# print(f"master     : {config.spark.master}")
# print(f"dq mode    : {config.quality.mode}")
# print(f"full_table : {config.full_table('bronze', 'orders')}")

# # --- Test 2 : charger depuis un YAML temporaire
# yaml_content = {
#     "env": "dev",
#     "catalog": "spark_platform_dev",
#     "storage": {
#         "raw_bucket": "s3://platform-dev/raw/",
#         "delta_bucket": "s3://platform-dev/delta/",
#     },
#     "spark": {"master": "local[*]", "shuffle_partitions": 8},
#     "quality": {"mode": "warn"},
# }

# tmp_path = Path("configs/test_dev.yaml")
# with open(tmp_path, "w") as f:
#     yaml.dump(yaml_content, f)

# config2 = AppConfig.load(tmp_path)

# print("\n=== Test 2 : config depuis YAML ===")
# print(f"env        : {config2.env}")
# print(f"catalog    : {config2.catalog}")
# print(f"raw_bucket : {config2.storage.raw_bucket}")
# print(f"full_table : {config2.full_table('gold', 'fact_orders')}")

# # Nettoyage
# tmp_path.unlink()
# print("\nYAML temporaire supprimé.")

from src.utils.config import AppConfig

dev  = AppConfig.load("configs/dev.yaml")
prod = AppConfig.load("configs/prod.yaml")

print(dev.catalog, dev.spark.master, dev.quality.mode)
print(prod.catalog, prod.spark.master, prod.quality.mode)
