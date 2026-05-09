"""
Productor Kafka — Viajes de Taxi NYC (Streaming)
=================================================
Procesamiento de Datos Masivos | ITESO

Genera registros sintéticos de viajes de taxi NYC de forma continua
y los publica en un topic de Kafka. Simula un flujo de datos en tiempo
real para demostraciones del pipeline de streaming.

Uso:
  python3 kafka_producer.py --broker kafka:9093 --topic taxi-trips --sleep 2

Dependencias:
  pip install kafka-python numpy faker
"""

import argparse
import json
import time
import random

import numpy as np
from faker import Faker

from kafka import KafkaProducer

# ─────────────────────────────────────────────
# Inicialización de Faker
# ─────────────────────────────────────────────
fake = Faker()


# ─────────────────────────────────────────────
# Generador de registros de taxi
# ─────────────────────────────────────────────
def generate_single_taxi_record() -> dict:
    """
    Genera un único registro de viaje de taxi con los 19 campos
    del esquema NYC Taxi.

    Inyecta datos sucios según las probabilidades definidas:
    - ~2% trip_distance negativa o cero
    - ~1% fare_amount negativa o cero
    - ~2% PULocationID/DOLocationID nulos
    - congestion_surcharge y airport_fee siempre None
    - ~1% duplicados (se maneja en el bucle principal)

    Returns:
        dict con los 19 campos del esquema de taxi NYC
    """
    # --- Timestamps de pickup y dropoff ---
    # Genera un pickup aleatorio en los últimos 30 días
    pickup_dt = fake.date_time_between(start_date="-30d", end_date="now")
    # Dropoff entre 3 y 90 minutos después del pickup
    duration_minutes = int(np.random.randint(3, 90))
    dropoff_dt = pickup_dt + __import__("datetime").timedelta(minutes=duration_minutes)

    # --- Campos numéricos con numpy ---
    vendor_id = int(np.random.choice([1, 2]))
    passenger_count = int(np.random.randint(1, 7))
    trip_distance = float(np.round(np.random.uniform(0.5, 40.0), 2))
    ratecode_id = int(np.random.choice([1, 2, 3, 4, 5, 6]))
    store_and_fwd_flag = fake.random_element(elements=("Y", "N"))

    # Ubicaciones de pickup y dropoff (1-265)
    pu_location_id = int(np.random.randint(1, 266))
    do_location_id = int(np.random.randint(1, 266))

    payment_type = int(np.random.choice([1, 2, 3, 4]))

    # --- Montos ---
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
    # Inyección de datos sucios
    # ─────────────────────────────────────────

    # ~2% trip_distance negativa o cero
    if random.random() < 0.02:
        trip_distance = float(np.random.choice([0.0, -5.5, -10.0]))

    # ~1% fare_amount negativa o cero
    if random.random() < 0.01:
        fare_amount = float(np.random.choice([0.0, -2.5]))
        # Recalcular total_amount con fare sucio
        total_amount = round(
            fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge, 2
        )

    # ~2% PULocationID nulo
    if random.random() < 0.02:
        pu_location_id = None

    # ~2% DOLocationID nulo
    if random.random() < 0.02:
        do_location_id = None

    # congestion_surcharge y airport_fee siempre None
    congestion_surcharge = None
    airport_fee = None

    # --- Construir registro final ---
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
# Bucle principal del productor
# ─────────────────────────────────────────────
def run_producer(broker: str, topic: str, sleep_seconds: float) -> None:
    """
    Bucle principal del productor Kafka.
    Crea un KafkaProducer con serialización JSON/UTF-8.
    Ejecuta un bucle infinito: genera registro, envía a Kafka, duerme.
    Maneja ~1% de duplicados enviando el mismo registro dos veces.

    Args:
        broker: Dirección del broker de Kafka (ej: kafka:9093)
        topic: Nombre del topic de Kafka (ej: taxi-trips)
        sleep_seconds: Segundos de pausa entre cada envío
    """
    # Crear productor con serialización JSON codificada en UTF-8
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
    )

    print(f"Conectado al broker  : {broker}")
    print(f"Topic                : {topic}")
    print(f"Intervalo de envío   : {sleep_seconds}s")
    print("-" * 55)

    count = 0
    try:
        while True:
            # Generar un registro de viaje de taxi
            record = generate_single_taxi_record()

            # Enviar registro al topic de Kafka
            producer.send(topic, value=record)
            producer.flush()
            count += 1
            print(f"[{count}] Enviado: VendorID={record['VendorID']}, "
                  f"PU={record['PULocationID']}, DO={record['DOLocationID']}, "
                  f"dist={record['trip_distance']}, fare={record['fare_amount']}")

            # ~1% de probabilidad de enviar duplicado (mismo registro otra vez)
            if random.random() < 0.01:
                producer.send(topic, value=record)
                producer.flush()
                count += 1
                print(f"[{count}] DUPLICADO enviado")

            # Pausa configurable entre envíos
            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        # Cierre limpio al recibir Ctrl+C
        print("\nInterrupción recibida. Cerrando productor...")
    finally:
        producer.close()
        print(f"Productor cerrado. Total de registros enviados: {count}")


# ─────────────────────────────────────────────
# CLI con argparse
# ─────────────────────────────────────────────
def build_parser() -> argparse.ArgumentParser:
    """Construye el parser de argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Productor Kafka que genera viajes de taxi NYC sintéticos en tiempo real."
    )
    parser.add_argument(
        "--broker",
        default="kafka:9093",
        help="Dirección del broker de Kafka (default: kafka:9093).",
    )
    parser.add_argument(
        "--topic",
        default="taxi-trips",
        help="Nombre del topic de Kafka (default: taxi-trips).",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=2,
        help="Segundos de pausa entre cada envío de mensaje (default: 2).",
    )
    return parser


def main():
    """Punto de entrada principal del script."""
    parser = build_parser()
    args = parser.parse_args()
    run_producer(broker=args.broker, topic=args.topic, sleep_seconds=args.sleep)


if __name__ == "__main__":
    main()
