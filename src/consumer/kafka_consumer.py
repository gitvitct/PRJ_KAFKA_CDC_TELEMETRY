import json
import logging
import time

from confluent_kafka import Consumer
from opentelemetry.trace import Status, StatusCode

from src.consumer.config import KAFKA_CONFIG
from src.consumer.analytics_writer import AnalyticsWriter
from src.consumer.telemetry import meter, setup_telemetry, tracer


TOPIC = "cdc.public.users"

setup_telemetry()

logger = logging.getLogger(__name__)

TRACER = tracer()
METER = meter()

MESSAGES_CONSUMED = METER.create_counter(
    "cdc.messages.consumed",
    description="Number of Kafka messages received by the CDC consumer.",
)

MESSAGES_PROCESSED = METER.create_counter(
    "cdc.messages.processed",
    description="Number of CDC messages successfully persisted.",
)

MESSAGES_FAILED = METER.create_counter(
    "cdc.messages.failed",
    description="Number of CDC messages that failed processing.",
)

PROCESSING_TIME = METER.create_histogram(
    "cdc.processing.duration",
    unit="ms",
    description="CDC message processing duration.",
)


class CDCConsumer:

    def __init__(self):
        self.consumer = Consumer(KAFKA_CONFIG)
        self.consumer.subscribe([TOPIC])
        self.writer = AnalyticsWriter()

        logger.info(
            "Subscribed to topic: %s",
            TOPIC,
        )

    def run(self):
        logger.info("CDC consumer loop started")

        while True:
            msg = self.consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error(
                    "Consumer error: %s",
                    msg.error(),
                )

                MESSAGES_FAILED.add(
                    1,
                    {
                        "reason": "kafka_consumer_error",
                    },
                )

                continue

            logger.info(
                "MESSAGE POLLED | topic=%s | partition=%s | offset=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

            MESSAGES_CONSUMED.add(
                1,
                {
                    "topic": msg.topic(),
                    "partition": str(msg.partition()),
                },
            )

            self._process_message(msg)

    def _process_message(self, msg):

        logger.info(
            "MESSAGE RECEIVED | topic=%s | partition=%s | offset=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )

        attributes = {
            "messaging.system": "kafka",
            "messaging.destination.name": msg.topic(),
            "messaging.kafka.partition": msg.partition(),
            "messaging.kafka.offset": msg.offset(),
            "messaging.destination_kind": "topic",
        }

        with TRACER.start_as_current_span(
            "cdc.process",
            attributes=attributes,
        ) as span:

            started = time.perf_counter()

            try:

                with TRACER.start_as_current_span(
                    "cdc.deserialize"
                ):

                #    data = json.loads(
                #       msg.value().decode("utf-8")
                #    )

                #    payload = data["payload"]


                    data = json.loads(
                        msg.value().decode("utf-8")
                    )

                    # TESTE CONTROLADO DE FALHA
                    if isinstance(data, dict) and data.get("test_error") is True:
                        raise RuntimeError("SIMULATED_CDC_ERROR")

                    payload = data["payload"]




                operation = payload["op"]

                source_table = payload["source"]["table"]

                after = payload.get("after")

                before = payload.get("before")

                record_id = None

                if after:
                    record_id = after.get("id")

                elif before:
                    record_id = before.get("id")

                logger.info(
                    "CDC PAYLOAD | op=%s | table=%s | id=%s",
                    operation,
                    source_table,
                    record_id,
                )

                span.set_attributes(
                    {
                        "cdc.operation": operation,
                        "cdc.source_table": source_table,
                        "cdc.record_id": (
                            record_id
                            if record_id is not None
                            else ""
                        ),
                    }
                )

                with TRACER.start_as_current_span(
                    "cdc.analytics.write"
                ):

                    self.writer.save_event(
                        operation,
                        source_table,
                        record_id,
                        payload,
                    )

                MESSAGES_PROCESSED.add(
                    1,
                    {
                        "operation": operation,
                        "source_table": source_table,
                    },
                )

                span.set_status(
                    Status(StatusCode.OK)
                )

                logger.info(
                    "CDC Event Saved | "
                    "op=%s | table=%s | id=%s | "
                    "partition=%s | offset=%s",
                    operation,
                    source_table,
                    record_id,
                    msg.partition(),
                    msg.offset(),
                )

            except Exception as exc:

                MESSAGES_FAILED.add(
                    1,
                    {
                        "reason": type(exc).__name__,
                    },
                )

                span.record_exception(exc)

                span.set_status(
                    Status(
                        StatusCode.ERROR,
                        str(exc),
                    )
                )

                logger.exception(
                    "Error processing message"
                )

            finally:

                duration_ms = (
                    time.perf_counter() - started
                ) * 1000

                PROCESSING_TIME.record(
                    duration_ms,
                    {
                        "topic": msg.topic(),
                    },
                )

                logger.info(
                    "CDC PROCESSING FINISHED | "
                    "topic=%s | partition=%s | offset=%s | "
                    "duration_ms=%.2f",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                    duration_ms,
                )