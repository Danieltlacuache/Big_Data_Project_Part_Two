"""Script rápido para verificar que generate_single_taxi_record funciona correctamente."""
import sys
import json

sys.path.insert(0, ".")
from kafka_producer import generate_single_taxi_record

# Generar un registro y verificar estructura
record = generate_single_taxi_record()

# Verificar 19 campos
expected_fields = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID", "store_and_fwd_flag",
    "PULocationID", "DOLocationID", "payment_type", "fare_amount",
    "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee"
]

assert len(record) == 19, f"Expected 19 fields, got {len(record)}"
for field in expected_fields:
    assert field in record, f"Missing field: {field}"

# Verificar que congestion_surcharge y airport_fee son None
assert record["congestion_surcharge"] is None, "congestion_surcharge should be None"
assert record["airport_fee"] is None, "airport_fee should be None"

# Verificar serialización JSON
json_str = json.dumps(record, default=str).encode("utf-8")
deserialized = json.loads(json_str.decode("utf-8"))
assert len(deserialized) == 19

print("OK: Registro generado correctamente con 19 campos")
print(f"OK: congestion_surcharge = {record['congestion_surcharge']}")
print(f"OK: airport_fee = {record['airport_fee']}")
print(f"OK: Serialización JSON ({len(json_str)} bytes)")

# Verificar distribución de datos sucios con 10000 registros
dirty_dist = 0
dirty_fare = 0
null_pu = 0
null_do = 0
total = 10000

for _ in range(total):
    r = generate_single_taxi_record()
    if r["trip_distance"] <= 0:
        dirty_dist += 1
    if r["fare_amount"] <= 0:
        dirty_fare += 1
    if r["PULocationID"] is None:
        null_pu += 1
    if r["DOLocationID"] is None:
        null_do += 1

print(f"\nDistribución de datos sucios ({total} registros):")
print(f"  trip_distance <= 0: {dirty_dist/total*100:.1f}% (esperado ~2%)")
print(f"  fare_amount <= 0:   {dirty_fare/total*100:.1f}% (esperado ~1%)")
print(f"  PULocationID null:  {null_pu/total*100:.1f}% (esperado ~2%)")
print(f"  DOLocationID null:  {null_do/total*100:.1f}% (esperado ~2%)")

print("\nEjemplo de registro:")
print(json.dumps(record, default=str, indent=2))
print("\nTodos los tests pasaron correctamente.")
