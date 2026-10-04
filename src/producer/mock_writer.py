import json
import time
import random
from confluent_kafka import Producer

KAFKA_BROKER = "localhost:9092"
TOPIC = "cdc.public.users.mock"

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})

def delivery_report(err, msg):
    if err is not None:
        print(f"[ERROR] Delivery failed: {err}")
    else:
        print(f"[OK] Message sent to {msg.topic()} [{msg.partition()}]")

def generate_user_event(user_id: int):
    names = ["Vitor", "Kafka", "Debezium", "Alice", "Bob"]
    emails = ["a@test.com", "b@test.com", "c@test.com"]

    event = {
        "id": user_id,
        "name": random.choice(names),
        "email": random.choice(emails),
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ")
    }

    return event

def run():
    print("Starting mock producer...")

    user_id = 1

    while True:
        event = generate_user_event(user_id)

        producer.produce(
            TOPIC,
            key=str(user_id),
            value=json.dumps(event),
            callback=delivery_report
        )

        producer.flush()

        print(f"Produced: {event}")

        user_id += 1
        time.sleep(2)

if __name__ == "__main__":
    run()