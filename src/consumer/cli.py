import logging

from src.consumer.kafka_consumer import CDCConsumer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)


def main():
    logging.info("Starting CDC Consumer")

    consumer = CDCConsumer()

    logging.info("CDC Consumer initialized")

    consumer.run()


if __name__ == "__main__":
    main()