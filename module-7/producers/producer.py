import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer

# Download NYC green taxi trip data (first 1000 rows)
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = ['lpep_pickup_datetime', 'lpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'passenger_count', 'trip_distance', 'tip_amount', 'total_amount']

df = pd.read_parquet(url, columns=columns)

df['passenger_count'] = df['passenger_count'].fillna(0).astype('int64')

def ride_serializer(ride_dict):
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)
t0 = time.time()

topic_name = 'green-trips'

for _, row in df.iterrows():
    ride_dict = {
        'lpep_pickup_datetime': row['lpep_pickup_datetime'].isoformat(),
        'lpep_dropoff_datetime': row['lpep_dropoff_datetime'].isoformat(),
        'PULocationID': int(row['PULocationID']),
        'DOLocationID': int(row['DOLocationID']),
        'passenger_count': int(row['passenger_count']),
        'trip_distance': float(row['trip_distance']),
        'tip_amount': float(row['tip_amount']),
        'total_amount': float(row['total_amount']),
    }
    producer.send(topic_name, value=ride_dict)
    print(f"Sent: {ride_dict}")

producer.flush()

t1 = time.time()
print(f'took {(t1 - t0):.2f} seconds')

