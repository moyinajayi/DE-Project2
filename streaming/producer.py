"""
Kafka Producer: Stream recent Chicago crime records from Socrata API.

Queries the Socrata JSON API for recent records and publishes them
to a Kafka topic one-by-one, simulating a real-time crime event stream.
"""

import json
import time
import os

import requests
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "chicago_crime_events"

# Socrata JSON API — fetch recent 1000 records ordered by date desc
SOCRATA_URL = (
    "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
    "?$order=date DESC&$limit=1000"
)

conf = {"bootstrap.servers": KAFKA_BOOTSTRAP}


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")


def produce_events():
    producer = Producer(conf)

    print(f"Fetching recent records from Socrata API ...")
    resp = requests.get(SOCRATA_URL, timeout=60)
    resp.raise_for_status()
    records = resp.json()
    print(f"Got {len(records)} records. Publishing to Kafka topic '{TOPIC}' ...")

    for i, record in enumerate(records):
        key = record.get("case_number", str(i))
        producer.produce(
            TOPIC,
            key=key.encode("utf-8"),
            value=json.dumps(record).encode("utf-8"),
            callback=delivery_report,
        )
        if (i + 1) % 100 == 0:
            producer.flush()
            print(f"  Published {i + 1}/{len(records)} events")
            time.sleep(0.5)  # pace the stream

    producer.flush()
    print(f"Done. Published {len(records)} events to '{TOPIC}'.")


if __name__ == "__main__":
    produce_events()
