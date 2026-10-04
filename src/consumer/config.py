KAFKA_BROKER = "kafka:29092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

KAFKA_CONFIG = {
    "bootstrap.servers": "kafka:29092",
    "group.id": "cdc-consumer-group",
    "auto.offset.reset": "earliest"
}

POSTGRES_CONFIG = {
    "host": "postgres_analytics",
    "port": 5432,
    "database": "analytics_db",
    "user": "analytics",
    "password": "analytics"
}