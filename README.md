# Kafka CDC & Telemetry Platform

A local, containerized **Change Data Capture (CDC)** and observability lab built with PostgreSQL, Apache Kafka, Debezium, Python, OpenTelemetry, Prometheus, Jaeger, and Grafana.

The project demonstrates how to capture database changes from an operational PostgreSQL database, publish them as Kafka events, process them with a Python consumer, persist an event audit trail in a separate analytics database, and observe processing behavior with traces and metrics.

> **Scope:** This repository is a local learning and demonstration environment. The Compose configuration uses single-node services and development credentials; it is not a production deployment template.

## Contents

- [Architecture](#architecture)
- [Technology stack](#technology-stack)
- [Repository layout](#repository-layout)
- [Prerequisites](#prerequisites)
- [Run locally](#run-locally)
- [Verify the CDC pipeline](#verify-the-cdc-pipeline)
- [Observability](#observability)
- [Useful commands](#useful-commands)
- [Configuration notes](#configuration-notes)
- [Troubleshooting](#troubleshooting)
- [Learning objectives](#learning-objectives)

## Architecture

```text
                         CDC source pipeline
┌──────────────────┐   PostgreSQL WAL   ┌────────────────────┐
│ PostgreSQL OLTP   │ ─────────────────► │ Debezium Connector │
│ app_db.users      │                    │ Kafka Connect      │
└──────────────────┘                    └─────────┬──────────┘
                                                  │
                                                  ▼
                                        ┌───────────────────┐
                                        │ Apache Kafka      │
                                        │ cdc.public.users  │
                                        └─────────┬─────────┘
                                                  │
                                                  ▼
                                        ┌───────────────────┐
                                        │ Python CDC         │
                                        │ Consumer           │
                                        └─────────┬─────────┘
                                                  │
                                                  ▼
                                        ┌───────────────────┐
                                        │ PostgreSQL         │
                                        │ analytics_db       │
                                        │ cdc_events         │
                                        └───────────────────┘

                         Telemetry pipeline
┌───────────────────┐  OTLP/gRPC  ┌──────────────────────┐
│ Python CDC         │ ──────────► │ OpenTelemetry        │
│ Consumer           │             │ Collector            │
└───────────────────┘             └───────┬────────┬─────┘
                                          │        │
                               traces     │        │ metrics
                                          ▼        ▼
                                     ┌────────┐ ┌────────────┐
                                     │ Jaeger │ │ Prometheus │
                                     └───┬────┘ └─────┬──────┘
                                         └──────┬─────┘
                                                ▼
                                           ┌─────────┐
                                           │ Grafana │
                                           └─────────┘
```

### Event lifecycle

1. An insert, update, or delete occurs on `public.users` in the source PostgreSQL database.
2. PostgreSQL's write-ahead log (WAL), configured for logical replication, exposes the change to Debezium.
3. Kafka Connect publishes the change to `cdc.public.users`.
4. The Python consumer reads the event, extracts the operation, source table, record ID, and payload, then writes the event to the analytics database.
5. The consumer emits OpenTelemetry traces and metrics through the Collector. Traces are sent to Jaeger; metrics are exposed for Prometheus and can be visualized in Grafana.

## Technology stack

| Component | Purpose |
|---|---|
| PostgreSQL 15 | Source OLTP database and analytics/event-store database |
| Apache Kafka | Event streaming and buffering |
| ZooKeeper | Kafka coordination for this Compose setup |
| Debezium / Kafka Connect | WAL-based change capture and connector runtime |
| Schema Registry | Included service for schema-management experiments |
| Python 3.11 | CDC consumer and synthetic data generator |
| Confluent Kafka Python client | Kafka consumer implementation |
| Psycopg2 | PostgreSQL access from Python |
| OpenTelemetry SDK | Application traces and metrics |
| OpenTelemetry Collector | Receives OTLP telemetry and routes it to backends |
| Prometheus | Scrapes Collector metrics |
| Jaeger | Distributed trace exploration |
| Grafana | Dashboards for CDC and telemetry |
| Docker Compose | Local orchestration |

The active connector configuration uses Kafka Connect's JSON converters. Avro-related configuration and the `schemas/users.avsc` file are included for experimentation, but Avro is not the default path in the current Compose configuration.

## Repository layout

```text
.
├── docker/
│   ├── docker-compose.yml
│   ├── .env                         # Local development settings
│   ├── bootstrap.sh                 # Optional setup helper
│   ├── Dockerfile.consumer
│   ├── Dockerfile.generator
│   ├── connect/
│   │   ├── debezium-connector.json
│   │   └── debezium-connector-avro.json
│   ├── postgres/init.sql            # Source table and sample records
│   ├── analytics/init.sql           # Analytics event table
│   ├── prometheus/prometheus.yml
│   └── otel/collector-config.yaml
├── grafana/
│   ├── dashboards/
│   │   ├── cdc_dashboard.json
│   │   └── otel_observability.json
│   └── provisioning/
├── schemas/users.avsc
├── src/
│   ├── consumer/
│   │   ├── cli.py
│   │   ├── config.py
│   │   ├── kafka_consumer.py
│   │   ├── analytics_writer.py
│   │   └── telemetry.py
│   ├── generator/generate_users.py
│   ├── producer/mock_writer.py
│   └── utils/
└── requirements.txt
```

## Prerequisites

- Docker Engine and Docker Compose v2 (`docker compose`)
- `curl` for connector registration and health checks
- Available local ports listed in the table below

You do not need a local Python environment to run the consumer or generator: both are built as containers. Python is only needed if you want to run or develop the application directly on your host.

## Run locally

Run these commands from the repository root.

### 1. Review local configuration

The Compose file reads environment variables from `docker/.env`. Review that file before starting the stack. The committed values are intended only for local development; replace them for any shared or exposed environment, and do not commit real credentials.

### 2. Start the stack

```bash
cd docker
docker compose up -d --build
```

This starts the source and analytics databases, Kafka and its supporting services, Kafka Connect, the consumer, the synthetic data generator, and the observability components.

Check service status and logs:

```bash
docker compose ps
docker compose logs -f kafka-connect cdc_consumer
```

### 3. Register the Debezium connector

Check whether the connector is already registered:

```bash
curl -s http://localhost:8083/connectors
```

If it is not listed, register the JSON connector from the `docker/` directory:

```bash
curl -i -X POST \
  -H 'Content-Type: application/json' \
  --data @connect/debezium-connector.json \
  http://localhost:8083/connectors
```

Inspect its status:

```bash
curl -s http://localhost:8083/connectors/postgres-users-connector/status
```

The connector name in the URL must match the `name` field in the connector configuration. A healthy connector should report a `RUNNING` state for its connector and task.

> If the connector configuration has already been registered, do not POST it again. Kafka Connect will return a conflict response. Inspect its status or update its configuration instead.

### 4. Confirm the consumer is running

The consumer is started by Compose. Follow its logs:

```bash
docker compose logs -f cdc_consumer
```

The consumer subscribes to `cdc.public.users` and writes processed events to `analytics_db.public.cdc_events`.

## Verify the CDC pipeline

The synthetic generator continuously performs inserts, updates, and deletes against the source database. You can also create a test change manually.

### Insert a test record

Open a source database shell:

```bash
docker exec -it postgres_cdc psql -U postgres -d app_db
```

Run:

```sql
INSERT INTO public.users (name, email)
VALUES ('CDC Test', 'cdc-test@example.com');
```

You can also test updates and deletes:

```sql
UPDATE public.users
SET email = 'cdc-test-updated@example.com'
WHERE email = 'cdc-test@example.com';

DELETE FROM public.users
WHERE email = 'cdc-test-updated@example.com';
```

### Inspect captured events

Open the analytics database:

```bash
docker exec -it postgres_analytics psql -U analytics -d analytics_db
```

Query the event table:

```sql
SELECT
    id,
    event_timestamp,
    operation,
    source_table,
    record_id,
    payload
FROM public.cdc_events
ORDER BY id DESC
LIMIT 20;
```

Debezium operation codes commonly used by this pipeline:

| Code | Meaning |
|---|---|
| `c` | Create / insert |
| `u` | Update |
| `d` | Delete |
| `r` | Snapshot read |

For deletes, the record identifier may be derived from the event's `before` image when `after` is null.

## Observability

### Local interfaces

| Component | URL | Use |
|---|---|---|
| Grafana | [http://localhost:3000](http://localhost:3000) | CDC and telemetry dashboards |
| Jaeger | [http://localhost:16686](http://localhost:16686) | Search and inspect distributed traces |
| Prometheus | [http://localhost:9090](http://localhost:9090) | Query scraped metrics and check targets |
| Kafka Connect REST API | [http://localhost:8083](http://localhost:8083) | Manage connectors and inspect status |
| Schema Registry | [http://localhost:8081](http://localhost:8081) | Schema Registry API |
| OTel Collector metrics | [http://localhost:8889/metrics](http://localhost:8889/metrics) | Collector-exported Prometheus metrics |

Grafana's local development credentials are configured in `docker/.env`. Change them before exposing the service beyond your machine.

### Traces

The consumer creates a `cdc.process` span for each message and child spans for deserialization and analytics persistence. Attributes include the Kafka topic, partition, offset, CDC operation, source table, and record ID. In Jaeger, select the `cdc-consumer` service and search for traces after generating changes.

### Application metrics

The consumer instruments these metrics:

- `cdc.messages.consumed` — messages received from Kafka.
- `cdc.messages.processed` — events successfully written to the analytics database.
- `cdc.messages.failed` — processing or consumer errors.
- `cdc.processing.duration` — message-processing duration in milliseconds.

The Collector exposes metrics at port `8889`, which Prometheus scrapes every five seconds. Metric names may be normalized by the OpenTelemetry Prometheus exporter; search the Prometheus UI for `cdc` or `otelcol` if a metric is not found under its original instrumentation name.

## Useful commands

Run from the `docker/` directory unless otherwise stated.

```bash
# Start or rebuild all services
docker compose up -d --build

# Show status
docker compose ps

# Follow application and connector logs
docker compose logs -f cdc_consumer kafka-connect

# Inspect a connector and its task status
curl -s http://localhost:8083/connectors/postgres-users-connector/status

# List Kafka topics from inside the broker container
docker exec -it kafka kafka-topics \
  --bootstrap-server kafka:29092 --list

# Stop containers but retain named volumes
docker compose down

# Stop containers and delete persisted local data (destructive)
docker compose down -v
```

## Configuration notes

- **Source database:** `app_db`, table `public.users`, container `postgres_cdc`, host port `5432`. Logical WAL settings are enabled for CDC.
- **Analytics database:** `analytics_db`, table `public.cdc_events`, container `postgres_analytics`, host port `5433`.
- **Kafka topic:** `cdc.public.users`.
- **Kafka Connect REST API:** host port `8083`.
- **Telemetry transport:** the consumer exports traces and metrics over OTLP/gRPC to `otel-collector:4317` on the Compose network. OTLP/HTTP is also exposed by the Collector on port `4318`.
- **Persistence:** named Docker volumes retain database, Grafana, and Prometheus data across ordinary container restarts. Initialization SQL is applied when the corresponding database volume is first created; changing an init script does not automatically rebuild an existing database.
- **Security:** the sample environment uses simple development credentials and unencrypted local service endpoints. Use secrets management, least-privilege access, TLS, and hardened network configuration for production deployments.

## Troubleshooting

**Connector is missing or not running**

```bash
curl -s http://localhost:8083/connectors
curl -s http://localhost:8083/connectors/postgres-users-connector/status
docker compose logs -f kafka-connect
```

Check that PostgreSQL is healthy, Kafka Connect can reach the broker, and the connector configuration matches the source database and replication settings.

**No events appear in Kafka**

Confirm that the connector task is `RUNNING`, the source table is `public.users`, and the topic is `cdc.public.users`. Then insert a new source row and inspect Kafka Connect logs.

**No rows appear in `cdc_events`**

Check the consumer logs, topic name, and analytics database connection. Confirm that the consumer can reach Kafka and `postgres_analytics` on the Compose network.

**No traces appear in Jaeger**

Check `docker compose logs -f cdc_consumer otel-collector jaeger`. Confirm the consumer's `OTEL_EXPORTER_OTLP_ENDPOINT` is `http://otel-collector:4317` and that the Collector trace pipeline exports to Jaeger.

**No metrics appear in Prometheus**

Open [Prometheus targets](http://localhost:9090/targets) and verify the `opentelemetry-collector` target is up. You can also inspect [the Collector metrics endpoint](http://localhost:8889/metrics).

**A configuration or initialization change does not take effect**

Recreate the affected container as needed. Database initialization scripts only run for a newly initialized data directory; deleting volumes with `docker compose down -v` resets local state and permanently removes persisted data.

## Learning objectives

This project is intended to provide hands-on practice with:

- Log-based Change Data Capture using PostgreSQL WAL and Debezium.
- Kafka topics, consumer groups, offsets, and event-driven integration.
- Python event processing and persistence into an analytics store.
- CDC operation handling for inserts, updates, deletes, and snapshots.
- OpenTelemetry instrumentation with spans, attributes, counters, and histograms.
- Collector pipelines and OTLP transport.
- Metrics scraping with Prometheus, trace exploration with Jaeger, and dashboards in Grafana.
- Docker Compose service orchestration, health checks, networking, and persistent volumes.

## License

Add a license file before distributing or reusing this project publicly. Until a license is specified, no additional license is implied.
