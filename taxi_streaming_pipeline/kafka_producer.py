"""
Kafka Producer — NYC Taxi Trips (Streaming)
============================================
Big Data Processing | ITESO

Continuously generates synthetic NYC taxi trip records and publishes
them to a Kafka topic. Simulates a real-time data flow for streaming
pipeline demonstrations.

Usage:
  python3 kafka_producer.py --broker kafka:9093 --topic taxi-trips --sleep 2

Dependencies:
  pip install kafka-python numpy faker

Neo4j query to see the complete graph:

  MATCH (n)
  OPTIONAL MATCH (n)-[r]->(m)
  RETURN n, r, m



  MATCH (n)-[r]->(m)
  RETURN n, r, m

Commands to inspect generated checkpoints:

  ls -la /opt/spark/work-dir/checkpoints/taxi_streaming_checkpoint/
  ls -la /opt/spark/work-dir/checkpoints/taxi_streaming_checkpoint/commits/
  ls -la /opt/spark/work-dir/checkpoints/taxi_streaming_checkpoint/offsets/
  ls -la /opt/spark/work-dir/checkpoints/taxi_streaming_checkpoint/sources/
  cat /opt/spark/work-dir/checkpoints/taxi_streaming_checkpoint/metadata
  cat /opt/spark/work-dir/checkpoints/taxi_streaming_checkpoint/offsets/0


  MATCH (n) DETACH DELETE n
"""

import argparse
import json
import time
import random

import numpy as np
from faker import Faker

from kafka import KafkaProducer

# ─────────────────────────────────────────────
# Faker initialization
# ─────────────────────────────────────────────
fake = Faker()


# ─────────────────────────────────────────────
# Taxi record generator
# ─────────────────────────────────────────────
def generate_single_taxi_record() -> dict:
    """
    Generates a single taxi trip record with the 19 fields
    of the NYC Taxi schema.

    Injects dirty data according to defined probabilities:
    - ~2% negative or zero trip_distance
    - ~1% negative or zero fare_amount
    - ~2% null PULocationID/DOLocationID
    - congestion_surcharge and airport_fee always None
    - ~1% duplicates (handled in the main loop)

    Returns:
        dict with the 19 fields of the NYC taxi schema
    """
    # --- Pickup and dropoff timestamps ---
    # Generate a random pickup within the last 30 days
    pickup_dt = fake.date_time_between(start_date="-30d", end_date="now")
    # Dropoff between 3 and 90 minutes after pickup
    duration_minutes = int(np.random.randint(3, 90))
    dropoff_dt = pickup_dt + __import__("datetime").timedelta(minutes=duration_minutes)

    # --- Numeric fields with numpy ---
    vendor_id = int(np.random.choice([1, 2]))
    passenger_count = int(np.random.randint(1, 7))
    trip_distance = float(np.round(np.random.uniform(0.5, 40.0), 2))
    ratecode_id = int(np.random.choice([1, 2, 3, 4, 5, 6]))
    store_and_fwd_flag = fake.random_element(elements=("Y", "N"))

    # Pickup and dropoff locations (1-265)
    pu_location_id = int(np.random.randint(1, 266))
    do_location_id = int(np.random.randint(1, 266))

    payment_type = int(np.random.choice([1, 2, 3, 4]))

    # --- Amounts ---
    fare_amount = float(np.round(np.random.uniform(3.0, 150.0), 2))
    extra = float(np.random.choice([0.0, 0.5, 1.0]))
    mta_tax = 0.5
    tip_amount = float(np.round(np.random.uniform(0.0, 25.0), 2))
    tolls_amount = float(np.random.choice([0.0, 0.0, 6.12, 12.24]))
    improvement_surcharge = 0.3

    total_amount = round(
        fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge, 2
    )

    # ─────────────────────────────────────────
    # Dirty data injection
    # ─────────────────────────────────────────

    # ~2% negative or zero trip_distance
    if random.random() < 0.02:
        trip_distance = float(np.random.choice([0.0, -5.5, -10.0]))

    # ~1% negative or zero fare_amount
    if random.random() < 0.01:
        fare_amount = float(np.random.choice([0.0, -2.5]))
        # Recalculate total_amount with dirty fare
        total_amount = round(
            fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge, 2
        )

    # ~2% null PULocationID
    if random.random() < 0.02:
        pu_location_id = None

    # ~2% null DOLocationID
    if random.random() < 0.02:
        do_location_id = None

    # congestion_surcharge and airport_fee always None
    congestion_surcharge = None
    airport_fee = None

    # --- Build final record ---
    record = {
        "VendorID": vendor_id,
        "tpep_pickup_datetime": str(pickup_dt),
        "tpep_dropoff_datetime": str(dropoff_dt),
        "passenger_count": passenger_count,
        "trip_distance": trip_distance,
        "RatecodeID": ratecode_id,
        "store_and_fwd_flag": store_and_fwd_flag,
        "PULocationID": pu_location_id,
        "DOLocationID": do_location_id,
        "payment_type": payment_type,
        "fare_amount": fare_amount,
        "extra": extra,
        "mta_tax": mta_tax,
        "tip_amount": tip_amount,
        "tolls_amount": tolls_amount,
        "improvement_surcharge": improvement_surcharge,
        "total_amount": total_amount,
        "congestion_surcharge": congestion_surcharge,
        "airport_fee": airport_fee,
    }

    return record


# ─────────────────────────────────────────────
# Producer main loop
# ─────────────────────────────────────────────
def run_producer(broker: str, topic: str, sleep_seconds: float) -> None:
    """
    Main loop of the Kafka producer.
    Creates a KafkaProducer with JSON/UTF-8 serialization.
    Runs an infinite loop: generate record, send to Kafka, sleep.
    Handles ~1% duplicates by sending the same record twice.

    Args:
        broker: Kafka broker address (e.g., kafka:9093)
        topic: Kafka topic name (e.g., taxi-trips)
        sleep_seconds: Seconds to pause between each send
    """
    # Create producer with JSON serialization encoded in UTF-8
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    print(f"Connected to broker  : {broker}")
    print(f"Topic                : {topic}")
    print(f"Send interval        : {sleep_seconds}s")
    print("-" * 55)

    count = 0
    try:
        while True:
            # Generate a taxi trip record
            record = generate_single_taxi_record()

            # Send record to Kafka topic
            producer.send(topic, value=record)
            producer.flush()
            count += 1
            print(f"[{count}] Sent: VendorID={record['VendorID']}, "
                  f"PU={record['PULocationID']}, DO={record['DOLocationID']}, "
                  f"dist={record['trip_distance']}, fare={record['fare_amount']}")

            # ~1% probability of sending a duplicate (same record again)
            if random.random() < 0.01:
                producer.send(topic, value=record)
                producer.flush()
                count += 1
                print(f"[{count}] DUPLICATE sent")

            # Configurable pause between sends
            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        # Clean shutdown on Ctrl+C
        print("\nInterrupt received. Closing producer...")
    finally:
        producer.close()
        print(f"Producer closed. Total records sent: {count}")


# ─────────────────────────────────────────────
# CLI with argparse
# ─────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    """Builds the command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="Kafka producer that generates synthetic NYC taxi trips in real time."
    )
    parser.add_argument(
        "--broker",
        default="kafka:9093",
        help="Kafka broker address (default: kafka:9093).",
    )
    parser.add_argument(
        "--topic",
        default="taxi-trips",
        help="Kafka topic name (default: taxi-trips).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=2,
        help="Seconds to pause between each message send (default: 2).",
    )
    return parser


def main():
    """Main entry point of the script."""
    parser = build_parser()
    args = parser.parse_args()
    run_producer(broker=args.broker, topic=args.topic, sleep_seconds=args.sleep)


if __name__ == "__main__":
    main()
