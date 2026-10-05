import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Producer

from src.consumer.config import KAFKA_CONFIG


logger = logging.getLogger(__name__)

DLQ_TOPIC = "cdc.public.users.DLQ"


class DLQProducer:

    def __init__(self):
        producer_config = {
            "bootstrap.servers": KAFKA_CONFIG["bootstrap.servers"]
        }

        self.producer = Producer(producer_config)

    def send(self, msg, error):

        dlq_payload = {
            "original_topic": msg.topic(),
            "original_partition": msg.partition(),
            "original_offset": msg.offset(),
            "error_type": type(error).__name__,
            "error_message": str(error),
            "payload": msg.value().decode("utf-8", errors="replace"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        self.producer.produce(
            DLQ_TOPIC,
            value=json.dumps(dlq_payload).encode("utf-8"),
        )

        self.producer.flush()

        logger.error(
            "MESSAGE SENT TO DLQ | "
            "topic=%s | partition=%s | offset=%s | error=%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            type(error).__name__,
        )

