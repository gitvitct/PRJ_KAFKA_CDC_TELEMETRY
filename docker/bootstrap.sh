#!/bin/bash

set -e

echo "========================================="
echo "Criando arquivo .env"
echo "========================================="

cat <<EOF > .env
# =========================
# PostgreSQL CDC
# =========================

CDC_POSTGRES_HOST=postgres
CDC_POSTGRES_USER=postgres
CDC_POSTGRES_PASSWORD=postgres
CDC_POSTGRES_PORT=5432
CDC_POSTGRES_SCHEMA=public
CDC_POSTGRES_DB=app_db

CDC_TOPIC_PREFIX=cdc				

# =========================
# PostgreSQL Analytics
# =========================

ANL_POSTGRES_HOST=postgres
ANL_POSTGRES_USER=analytics
ANL_POSTGRES_PASSWORD=analytics
ANL_POSTGRES_PORT=5433
ANL_POSTGRES_SCHEMA=public
ANL_POSTGRES_DB=analytics_db

# =========================
# GRAFANA
# =========================

GF_SECURITY_ADMIN_USER= admin
GF_SECURITY_ADMIN_PASSWORD= admin
EOF

echo ".env criado com sucesso."


echo ""
echo "========================================="
echo "Subindo containers"
echo "========================================="

docker compose up -d

echo ""
echo "========================================="
echo "Aguardando Kafka Connect iniciar..."
echo "========================================="

#until curl -s http://kafka-connect:8083/connectors > /dev/null
until curl -s http://localhost:8083/connectors > /dev/null
do
  echo "Kafka Connect ainda não disponível..."
  sleep 5
done

echo ""
echo "========================================="
echo "Registrando conector Debezium..."
echo "========================================="

curl -X POST \
-H "Content-Type: application/json" \
http://localhost:8083/connectors \
-d @connect/debezium-connector.json	

echo "Conector registrado!"

echo ""
echo "========================================="
echo "Bootstrap concluído"
echo "========================================="
echo ""
echo "Grafana:"
echo "  URL      : http://localhost:3000"
echo "  Usuário  : admin"
echo "  Senha    : admin"
echo ""
