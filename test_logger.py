from src.utils.logger import get_logger

logger = get_logger(__name__)

# Log simple
logger.info("Démarrage du job Bronze")

# Log avec contexte métier
logger.info("Ingestion terminée", extra={"table": "bronze.orders", "rows_written": 99441})

# Log d'avertissement
logger.warning("Lignes corrompues détectées", extra={"table": "bronze.orders", "quarantine_rows": 12})

# Log d'erreur avec exception
try:
    raise ValueError("Fichier CSV introuvable : olist_orders.csv")
except ValueError as e:
    logger.error("Échec de l'ingestion", exc_info=True, extra={"table": "bronze.orders"})
