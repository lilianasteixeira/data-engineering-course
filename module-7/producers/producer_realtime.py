import json
import sys
import time
import random
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import pandas as pd
from kafka import KafkaProducer

# ────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────
KAFKA_BOOTSTRAP = 'localhost:9092'          # or 'redpanda:29092' if inside Docker network
TOPIC_NAME = 'green-trips'

# Data source (change month/year if you want different file)
PARQUET_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"

# Columns we need (matches your Flink source DDL)
COLUMNS = [
    'lpep_pickup_datetime', 'lpep_dropoff_datetime',
    'PULocationID', 'DOLocationID',
    'passenger_count', 'trip_distance',
    'tip_amount', 'total_amount'
]

# How fast to send (seconds between messages)
MIN_DELAY = 0.3
MAX_DELAY = 1.2

# Probability of making an event "late" (timestamp shifted back)
LATE_PROBABILITY = 0.20
LATE_MIN_SEC = 3
LATE_MAX_SEC = 10

# ────────────────────────────────────────────────
# Load data once
# ────────────────────────────────────────────────
print("Loading green taxi data...")
df = pd.read_parquet(PARQUET_URL, columns=COLUMNS)

# Fix passenger_count (NaN → 0, to int)
df['passenger_count'] = df['passenger_count'].fillna(0).astype('int64')

print(f"Loaded {len(df):,} trips. Starting streaming producer...")
print("Press Ctrl+C to stop\n")

# ────────────────────────────────────────────────
# Kafka producer
# ────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

count = 0
start_time = time.time()

try:
    for _, row in df.iterrows():
        # Decide if this event should be late
        delay_sec = 0
        if random.random() < LATE_PROBABILITY:
            delay_sec = random.randint(LATE_MIN_SEC, LATE_MAX_SEC)

        # Convert real timestamps to epoch milliseconds
        pickup_dt = row['lpep_pickup_datetime']
        dropoff_dt = row['lpep_dropoff_datetime']

        pickup_ms = int(pickup_dt.timestamp() * 1000) - delay_sec * 1000
        dropoff_ms = int(dropoff_dt.timestamp() * 1000)

        ride_dict = {
            'lpep_pickup_datetime': pickup_ms,
            'lpep_dropoff_datetime': dropoff_ms,
            'PULocationID': int(row['PULocationID']),
            'DOLocationID': int(row['DOLocationID']),
            'passenger_count': int(row['passenger_count']),
            'trip_distance': float(row['trip_distance']),
            'tip_amount': float(row['tip_amount']),
            'total_amount': float(row['total_amount']),
        }

        # Print status (mimics your original script)
        ts = datetime.fromtimestamp(pickup_ms / 1000, tz=timezone.utc)
        if delay_sec > 0:
            print(f" LATE ({delay_sec}s) → PU={ride_dict['PULocationID']}  ts={ts:%Y-%m-%d %H:%M:%S}")
        else:
            print(f" on time      → PU={ride_dict['PULocationID']}  ts={ts:%Y-%m-%d %H:%M:%S}")

        # Send to Redpanda
        producer.send(TOPIC_NAME, value=ride_dict)
        count += 1

        # Simulate streaming arrival rate
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

except KeyboardInterrupt:
    producer.flush()
    elapsed = time.time() - start_time
    print(f"\nStopped. Sent {count:,} events in {elapsed:.2f} seconds.")
    print("Goodbye.")

finally:
    producer.close()