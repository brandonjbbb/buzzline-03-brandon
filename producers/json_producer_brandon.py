# File: producers/json_producer_brandon.py

from kafka import KafkaProducer
import json
import logging
import time

# Logger
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("json_producer")

# ---- Configure these if needed ----
TOPIC = "json-messages"        # your Kafka topic
BOOTSTRAP = "localhost:9092"   # your Kafka broker address
# -----------------------------------

def main():
    # Kafka producer that sends JSON
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    # <<< YOUR CUSTOM MESSAGES GO HERE >>>
    messages = [
        {"text": "I love breakfast for dinner."},
        {"text": "Walking and running is fun."},
        {"text": "I wish I could travel to Hawaii."}
    ]
    # <<< END CUSTOM MESSAGES >>>

    for msg in messages:
        producer.send(TOPIC, msg)
        log.info("Sent: %s", msg)
        time.sleep(1)  # small delay so you can see them one-by-one

    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()
