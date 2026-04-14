"""
Kafka Consumer: Read crime events from Kafka and write to BigQuery.

Consumes messages from the chicago_crime_events topic, batches them,
and inserts into BigQuery streaming table.
"""

import json
import os

from confluent_kafka import Consumer, KafkaError
from google.cloud import bigquery

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "chicago_crime_events"
GROUP_ID = "chicago-crime-bq-consumer"
BQ_TABLE = os.environ.get("BQ_TABLE", "chicago_crime.chicago_crime_stream")

conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
}


def consume_and_load():
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    client = bigquery.Client()
    batch = []
    batch_size = 100

    print(f"Consuming from '{TOPIC}' -> BigQuery '{BQ_TABLE}' ...")

    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                if batch:
                    _flush_batch(client, batch)
                    batch = []
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    if batch:
                        _flush_batch(client, batch)
                        batch = []
                    print("Reached end of partition, waiting ...")
                    continue
                raise Exception(msg.error())

            record = json.loads(msg.value().decode("utf-8"))
            row = {
                "id": record.get("id"),
                "case_number": record.get("case_number"),
                "date": record.get("date"),
                "primary_type": record.get("primary_type"),
                "description": record.get("description"),
                "location_description": record.get("location_description"),
                "arrest": record.get("arrest"),
                "domestic": record.get("domestic"),
                "beat": record.get("beat"),
                "district": record.get("district"),
                "ward": record.get("ward"),
                "latitude": record.get("latitude"),
                "longitude": record.get("longitude"),
                "year": record.get("year"),
            }
            batch.append(row)

            if len(batch) >= batch_size:
                _flush_batch(client, batch)
                batch = []

    except KeyboardInterrupt:
        print("Shutting down consumer ...")
    finally:
        if batch:
            _flush_batch(client, batch)
        consumer.close()


def _flush_batch(client, batch):
    errors = client.insert_rows_json(BQ_TABLE, batch)
    if errors:
        print(f"BigQuery insert errors: {errors}")
    else:
        print(f"Inserted {len(batch)} rows into {BQ_TABLE}")


if __name__ == "__main__":
    consume_and_load()
