Kafka CDC Telemetry Platform

A production-oriented Change Data Capture (CDC) and observability lab built around PostgreSQL, Debezium, Apache Kafka, Python, OpenTelemetry, Prometheus, Jaeger, and Grafana.

The project demonstrates an end-to-end event-driven data pipeline in which database changes are captured from PostgreSQL's WAL, published to Kafka by Debezium, consumed by a Python application, persisted into an analytics database, and instrumented with distributed traces and application metrics.

Purpose: Senior Data Engineer / Data Platform interview project focused on CDC, Kafka, Debezium, observability, reliability, and cloud-ready architecture concepts.

Architecture

                         CDC / Streaming Layer
┌───────────────┐
│ PostgreSQL    │
│ app_db        │
│ public.users  │
└───────┬───────┘
        │
        │ PostgreSQL WAL / logical replication
        ▼
┌─────────────────────┐
│ Debezium            │
│ PostgreSQL Connector│
└──────────┬──────────┘
           │
           │ CDC events
           ▼
┌─────────────────────┐
│ Apache Kafka        │
│ topic:              │
│ cdc.public.users    │
└──────────┬──────────┘
           │
           │ Kafka consumer
           ▼
┌──────────────────────────────┐
│ Python CDC Consumer          │
│                              │
│ - Deserialize event          │
│ - Extract CDC metadata       │
│ - Persist analytics event    │
│ - Emit metrics               │
│ - Create OpenTelemetry spans │
└───────────┬──────────────────┘
            │
            │ INSERT
            ▼
┌──────────────────────────────┐
│ PostgreSQL Analytics         │
│ analytics_db                 │
│ table: cdc_events            │
└──────────────────────────────┘


                    Observability Layer
                    ===================

        Python CDC Consumer
                │
                │ OTLP / gRPC
                ▼
     ┌─────────────────────────┐
     │ OpenTelemetry Collector │
     └───────────┬─────────────┘
                 │
          ┌──────┴───────┐
          │              │
          ▼              ▼
      ┌───────┐      ┌────────────┐
      │ Jaeger│      │ Prometheus │
      │Traces │      │  Metrics   │
      └───────┘      └─────┬──────┘
                            │
                            ▼
                       ┌─────────┐
                       │ Grafana │
                       └─────────┘

What This Project Demonstrates

CDC

PostgreSQL logical replication

WAL-based change capture

Debezium PostgreSQL connector

Snapshot + streaming CDC

Create / Update / Delete events

Kafka topic-based event distribution

Kafka

Kafka broker and topic

Consumer groups

Partition / offset metadata

Durable event streaming

Kafka Connect internal topics

Consumer processing metrics

Data Engineering

OLTP → CDC → Streaming → Analytics

Event-driven data pipelines

JSON CDC event processing

Analytics event persistence

Source metadata and record identification

Observability

OpenTelemetry SDK

OTLP/gRPC

Distributed tracing

Application metrics

OpenTelemetry Collector

Prometheus

Jaeger

Grafana dashboards

Error and latency instrumentation

Platform / DevOps

Docker Compose

Containerized local environment

Health checks

Service dependencies

Persistent Docker volumes

Environment-based configuration

Technology Stack

Component

Technology

Source database

PostgreSQL 15

Target database

PostgreSQL 15

CDC

Debezium 2.5

Streaming

Apache Kafka

Kafka coordination

Apache ZooKeeper

Kafka integration

Kafka Connect

Schema management

Confluent Schema Registry

Application

Python 3.11

Kafka client

confluent-kafka

PostgreSQL client

psycopg2

Telemetry

OpenTelemetry SDK

Telemetry pipeline

OpenTelemetry Collector

Tracing

Jaeger

Metrics

Prometheus

Visualization

Grafana

Runtime

Docker / Docker Compose

Project Structure

project_KAFKA_CDC_TELEMETRY/
│
├── docker/
│   ├── docker-compose.yml
│   ├── bootstrap.sh
│   ├── Dockerfile.consumer
│   ├── Dockerfile.generator
│   │
│   ├── postgres/
│   │   └── init.sql
│   │
│   ├── analytics/
│   │   └── init.sql
│   │
│   ├── connect/
│   │   ├── debezium-connector.json
│   │   └── debezium-connector-avro.json
│   │
│   ├── otel/
│   │   └── collector-config.yaml
│   │
│   └── prometheus/
│       └── prometheus.yml
│
├── grafana/
│   ├── dashboards/
│   │   ├── cdc_dashboard.json
│   │   └── otel_observability.json
│   │
│   └── provisioning/
│       ├── dashboards/
│       │   └── dashboard.yml
│       │
│       └── datasources/
│           ├── datasource.yml
│           └── observability.yml
│
├── schemas/
│   └── users.avsc
│
├── src/
│   ├── consumer/
│   │   ├── cli.py
│   │   ├── config.py
│   │   ├── kafka_consumer.py
│   │   ├── analytics_writer.py
│   │   └── telemetry.py
│   │
│   ├── generator/
│   │   └── generate_users.py
│   │
│   ├── producer/
│   │   └── mock_writer.py
│   │
│   └── utils/
│       ├── json_formatter.py
│       └── logger.py
│
└── README.md

End-to-End Data Flow

1. Source transaction

The source database contains:

public.users

Example:

INSERT INTO users(name, email)
VALUES ('Maria', 'maria@example.com');

or:

UPDATE users
SET name = 'Maria Silva'
WHERE id = 1;

or:

DELETE FROM users
WHERE id = 1;

2. PostgreSQL WAL

PostgreSQL is configured with:

wal_level = logical

This enables logical replication information to be generated in the Write-Ahead Log.

Debezium consumes this change stream through PostgreSQL logical replication.

3. Debezium

The connector monitors:

public.users

using:

plugin.name = pgoutput

and publishes CDC events using the topic prefix:

cdc

Therefore the resulting Kafka topic is:

cdc.public.users

The connector uses:

snapshot.mode = initial

which means an initial snapshot can be taken before Debezium continues with streaming changes.

4. Kafka

The event is published to:

cdc.public.users

The Python consumer uses:

group.id = cdc-consumer-group

and reads from the earliest available offset when no committed offset exists:

auto.offset.reset = earliest

The consumer also records Kafka metadata such as:

topic

partition

offset

These values are included in the OpenTelemetry trace attributes.

5. Python CDC Consumer

The consumer:

Polls Kafka.

Validates the Kafka message.

Deserializes the JSON payload.

Extracts the Debezium payload.

Identifies the CDC operation.

Extracts the source table.

Determines the record ID.

Writes the event to PostgreSQL Analytics.

Emits OpenTelemetry metrics.

Creates an OpenTelemetry trace.

The main processing span is:

cdc.process

with child spans:

cdc.deserialize
cdc.analytics.write

6. Analytics Database

Processed events are stored in:

analytics_db.cdc_events

Schema:

CREATE TABLE IF NOT EXISTS cdc_events
(
    id SERIAL PRIMARY KEY,
    event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    operation VARCHAR(10),
    source_table VARCHAR(100),
    record_id INTEGER,
    payload JSONB
);

This provides an audit-friendly representation of the CDC stream.

Debezium Event Model

A typical event has the following conceptual structure:

{
  "payload": {
    "before": null,
    "after": {
      "id": 1,
      "name": "Maria",
      "email": "maria@example.com"
    },
    "op": "c",
    "source": {
      "table": "users"
    }
  }
}

The important Debezium operation codes are:

Code

Meaning

c

Create / Insert

u

Update

d

Delete

r

Read / Snapshot

The consumer maps these events into the analytics table.

OpenTelemetry

The CDC consumer is instrumented using the OpenTelemetry Python SDK.

Telemetry is exported using:

OTLP/gRPC

to:

otel-collector:4317

The service identifies itself as:

service.name = cdc-consumer

and uses:

service.version = 1.0.0
deployment.environment = local

Trace Model

A CDC message produces a trace similar to:

cdc.process
│
├── cdc.deserialize
│
└── cdc.analytics.write

The parent span includes attributes such as:

messaging.system
messaging.destination.name
messaging.kafka.partition
messaging.kafka.offset
messaging.destination_kind
cdc.operation
cdc.source_table
cdc.record_id

This makes it possible to correlate:

Kafka message
      ↓
CDC processing
      ↓
deserialization
      ↓
analytics database write

Application Metrics

The consumer exposes OpenTelemetry metrics including:

Messages consumed

cdc.messages.consumed

Number of Kafka messages received.

Messages processed

cdc.messages.processed

Number of CDC messages successfully persisted.

Messages failed

cdc.messages.failed

Number of processing failures.

Processing duration

cdc.processing.duration

Histogram measuring message processing time in milliseconds.

These metrics are exported to the OpenTelemetry Collector and exposed to Prometheus.

Observability Pipeline

             OTLP
Python ──────────────────► OpenTelemetry Collector
                               │
                ┌──────────────┴──────────────┐
                │                             │
                ▼                             ▼
             Jaeger                      Prometheus
             Traces                       Metrics
                │                             │
                └──────────────┬──────────────┘
                               ▼
                            Grafana

The Collector configuration defines separate pipelines for:

traces
metrics

It also applies:

memory_limiter
batch

processors.

Local Endpoints

After the stack is running:

Service

URL

Purpose

Grafana

http://localhost:3000

Dashboards

Jaeger

http://localhost:16686

Distributed traces

Prometheus

http://localhost:9090

Metrics / PromQL

Kafka Connect

http://localhost:8083

Connector REST API

Schema Registry

http://localhost:8081

Schema management

OTel Collector metrics

http://localhost:8889/metrics

Collector metrics

PostgreSQL CDC

localhost:5432

Source database

PostgreSQL Analytics

localhost:5433

Analytics database

Kafka

localhost:9092

Kafka broker

Quick Start

Prerequisites

Install:

Docker

Docker Compose

curl

Git

Verify:

docker --version
docker compose version
curl --version

Start the Environment

From the project root:

cd docker

The project provides a bootstrap script that creates the local .env, starts the stack, waits for Kafka Connect, and registers the Debezium connector:

./bootstrap.sh

Alternatively, start the infrastructure manually:

docker compose up -d --build

Then register the connector:

curl -X POST \
  -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  -d @connect/debezium-connector.json

Verify the Environment

Check containers:

docker ps

Check Kafka Connect:

curl http://localhost:8083/connectors

Check the Debezium connector:

curl http://localhost:8083/connectors/postgres-users-connector/status

Expected state:

RUNNING

Generate CDC Events

The project includes a continuously running data generator.

It randomly performs:

INSERT  60%
UPDATE  30%
DELETE  10%

with a delay between operations.

You can also generate events manually.

Connect to the source database:

docker exec -it postgres_cdc \
  psql -U postgres -d app_db

Insert:

INSERT INTO users(name, email)
VALUES ('Kafka CDC Test', 'cdc@example.com');

Update:

UPDATE users
SET name = 'Kafka CDC Updated'
WHERE email = 'cdc@example.com';

Delete:

DELETE FROM users
WHERE email = 'cdc@example.com';

Verify CDC in Kafka

Inspect the topic from the Kafka container:

docker exec -it kafka \
  kafka-topics --bootstrap-server kafka:29092 --list

The expected CDC topic is:

cdc.public.users

Consume events:

docker exec -it kafka \
  kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic cdc.public.users \
  --from-beginning

Verify Analytics Persistence

Connect to the analytics database:

docker exec -it postgres_analytics \
  psql -U analytics -d analytics_db

Query CDC events:

SELECT
    id,
    event_timestamp,
    operation,
    source_table,
    record_id,
    payload
FROM cdc_events
ORDER BY id DESC
LIMIT 20;

Observe Traces in Jaeger

Open:

http://localhost:16686

Search for the service:

cdc-consumer

Look for spans such as:

cdc.process
cdc.deserialize
cdc.analytics.write

A trace should allow you to follow the processing of an individual Kafka event.

Observe Metrics in Prometheus

Open:

http://localhost:9090

Example queries:

rate(cdc_messages_processed_total[1m])

rate(cdc_messages_failed_total[1m])

sum(cdc_messages_processed_total)

For processing latency:

histogram_quantile(
  0.95,
  sum(
    rate(cdc_processing_duration_milliseconds_bucket[5m])
  ) by (le)
)

Grafana

Open:

http://localhost:3000

The Docker Compose configuration provisions Grafana dashboards and data sources.

The OpenTelemetry dashboard includes panels for:

CDC messages processed per second

CDC messages failed per second

CDC processing duration

Total messages processed

The CDC dashboard provides CDC-oriented operational visibility.

Testing Error Handling

The consumer contains a controlled failure mechanism.

A message containing:

{
  "test_error": true
}

triggers:

SIMULATED_CDC_ERROR

The exception is:

recorded in the OpenTelemetry span

marked as an error

logged by the consumer

counted by cdc.messages.failed

This provides a simple way to demonstrate failure observability during an interview or local test.

Avro / Schema Registry

The environment also includes Confluent Schema Registry:

http://localhost:8081

and the repository contains:

schemas/users.avsc

There is an alternative connector configuration:

docker/connect/debezium-connector-avro.json

The default connector currently uses Kafka Connect JSON converters:

JsonConverter

The Avro connector configuration is available as an alternative for demonstrating schema-based serialization with Schema Registry.

Configuration

The main environment variables are:

CDC_POSTGRES_HOST
CDC_POSTGRES_USER
CDC_POSTGRES_PASSWORD
CDC_POSTGRES_PORT
CDC_POSTGRES_DB

ANL_POSTGRES_HOST
ANL_POSTGRES_USER
ANL_POSTGRES_PASSWORD
ANL_POSTGRES_PORT
ANL_POSTGRES_DB

GF_SECURITY_ADMIN_USER
GF_SECURITY_ADMIN_PASSWORD

OpenTelemetry variables used by the consumer include:

OTEL_SERVICE_NAME
OTEL_SERVICE_VERSION
OTEL_ENVIRONMENT
OTEL_EXPORTER_OTLP_ENDPOINT

For local Docker execution, the consumer sends telemetry to:

http://otel-collector:4317

Useful Docker Commands

Start:

docker compose up -d

Rebuild:

docker compose up -d --build

Stop:

docker compose down

Stop and remove volumes:

docker compose down -v

Follow consumer logs:

docker logs -f cdc_consumer

Follow Debezium / Kafka Connect logs:

docker logs -f kafka_connect

Follow OpenTelemetry Collector logs:

docker logs -f otel_collector

Follow Prometheus logs:

docker logs -f prometheus

Troubleshooting

Kafka Connect is not available

Check:

docker logs kafka_connect

Then:

curl http://localhost:8083/

Connector is not running

Check:

curl http://localhost:8083/connectors/postgres-users-connector/status

and inspect:

docker logs kafka_connect

Common areas to verify:

PostgreSQL is healthy

wal_level=logical

connector configuration

replication slot

publication

Kafka broker availability

No CDC events are arriving

Check the topic:

docker exec -it kafka \
  kafka-topics --bootstrap-server kafka:29092 --list

Then inspect the consumer:

docker logs -f cdc_consumer

Also verify the source table:

SELECT * FROM public.users;

No traces in Jaeger

Verify the Collector:

docker logs -f otel_collector

Verify that the consumer has:

OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317

Verify Jaeger:

http://localhost:16686

No metrics in Prometheus

Check the Collector endpoint:

curl http://localhost:8889/metrics

Then verify Prometheus targets:

http://localhost:9090/targets

The configured target is:

otel-collector:8889

Security Notes

This repository is designed as a local development / interview laboratory.

The default credentials in the Docker configuration are intentionally simple:

postgres / postgres
analytics / analytics
admin / admin

Do not use these credentials in production.

For a production implementation, replace them with:

Docker secrets / Kubernetes Secrets

Azure Key Vault

AWS Secrets Manager

HashiCorp Vault

managed identity / workload identity

TLS for Kafka and databases

authentication and authorization

network isolation

least-privilege RBAC

Also avoid committing real .env files or credentials to Git.

Production Evolution

This local architecture can be evolved into a cloud-native platform:

PostgreSQL
    │
    ▼
Debezium / Kafka Connect
    │
    ▼
Kafka / Azure Event Hubs
    │
    ├──────────────► Stream Processing
    │                ├── Databricks
    │                └── Flink
    │
    ▼
Data Lake / Data Warehouse
    │
    ├── ADLS Gen2
    ├── Snowflake
    └── Azure PostgreSQL

The observability layer can evolve toward:

OpenTelemetry
      │
      ├── Metrics → Prometheus / Azure Monitor
      ├── Traces  → Jaeger / Application Insights
      └── Logs    → centralized logging platform

Infrastructure can subsequently be managed with:

Terraform
+
Kubernetes / AKS
+
CI/CD
+
Secrets Management

Senior Data Engineer Interview Topics Covered

This project can be used to discuss:

CDC

What is Change Data Capture?

Why use WAL-based CDC?

Snapshot vs streaming

Logical replication

Replication slots

Publications

Idempotency

Deletes and tombstones

Schema evolution

Debezium

Connector lifecycle

Snapshot modes

Offset management

Schema history

PostgreSQL pgoutput

Connector failure and restart

Replication slot management

Kafka

Topic / partition / offset

Consumer groups

Ordering

Consumer lag

At-least-once processing

Rebalancing

Retention

Replication factor

Backpressure

Observability

Metrics vs logs vs traces

OpenTelemetry

OTLP

Collector pipelines

Prometheus scraping

Distributed tracing

Trace context

Error instrumentation

Latency percentiles

Production Architecture

HA Kafka

Multiple Kafka Connect workers

Connector scaling

Dead Letter Queues

Retry strategies

Idempotent consumers

Schema Registry

Security

TLS / SASL

Secrets management

Kubernetes

Infrastructure as Code

Design Decisions

Why Debezium?

Debezium provides a standardized way to capture database changes from PostgreSQL without coupling the source application to the downstream consumers.

This separates:

OLTP application

from:

CDC / streaming consumers

Why Kafka?

Kafka provides a durable event streaming layer between the source and consumers.

This enables:

decoupling

replay

multiple consumers

scalable processing

consumer groups

independent downstream systems

Why OpenTelemetry?

OpenTelemetry keeps instrumentation independent from a specific observability backend.

The same application instrumentation can therefore be routed to different platforms without changing the business logic.

Limitations of the Current Lab

This project intentionally keeps the environment simple for local development.

It currently uses:

single Kafka broker

replication factor 1

local ZooKeeper

local PostgreSQL

local Docker networking

plaintext Kafka

simple credentials

direct PostgreSQL persistence

no production-grade DLQ

no multi-node failover

no Kubernetes deployment

These are appropriate trade-offs for a local technical demonstration, but would need to be addressed for production.

Roadmap

Potential next iterations:

Kafka replication factor > 1

Kafka Connect distributed mode

Dead Letter Queue

Retry / backoff strategy

Idempotent analytics writes

Consumer lag monitoring

Kafka exporter

Avro + Schema Registry as default

Schema evolution tests

Terraform infrastructure

Kubernetes / AKS deployment

Azure Event Hubs integration

ADLS Gen2 sink

Snowflake sink

CI/CD with GitHub Actions

Secrets management with Azure Key Vault

TLS / SASL authentication

Automated integration tests

Load testing and throughput benchmarks

Learning Outcome

This project provides a compact but realistic demonstration of a modern streaming data platform:

PostgreSQL
    ↓
Logical Replication / WAL
    ↓
Debezium
    ↓
Kafka
    ↓
Python Consumer
    ↓
Analytics PostgreSQL

with an independent observability path:

Python Consumer
    ↓
OpenTelemetry
    ↓
OTel Collector
    ├──→ Jaeger
    └──→ Prometheus
              ↓
           Grafana

The architecture demonstrates how data movement and system observability can be designed together, which is an important concern in production-grade Data Engineering platforms.

Author

Vitor Melo

Data Engineer | Data Platform & Analytics

Focus areas:

Data Engineering

Data Platforms

CDC

Kafka

Debezium

Azure

Snowflake

dbt

Airflow

Observability

Cloud Data Architecture