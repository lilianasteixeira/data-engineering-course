import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from kafka import KafkaConsumer

server = 'localhost:9092'
topic_name = 'green-trips'

# Connect to PostgreSQL
conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='postgres'
)
conn.autocommit = True
cur = conn.cursor()

# Create table if it doesn't exist
cur.execute("""
    CREATE TABLE IF NOT EXISTS processed_events (
        lpep_pickup_datetime TIMESTAMP,
        lpep_dropoff_datetime TIMESTAMP,
        PULocationID INTEGER,
        DOLocationID INTEGER,
        passenger_count INTEGER,
        trip_distance FLOAT,
        tip_amount FLOAT,
        total_amount FLOAT
    )
""")

def ride_deserializer(data):
    json_str = data.decode('utf-8')
    return json.loads(json_str)

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-to-postgres',
    value_deserializer=ride_deserializer
)

print(f"Listening to {topic_name} and writing to PostgreSQL...")

count = 0
for message in consumer:
    ride = message.value
    pickup_dt = datetime.fromisoformat(ride['lpep_pickup_datetime'])
    cur.execute(
        """INSERT INTO processed_events
           (lpep_pickup_datetime, lpep_dropoff_datetime, PULocationID, DOLocationID, passenger_count, trip_distance, tip_amount, total_amount)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        (ride['lpep_pickup_datetime'], ride['lpep_dropoff_datetime'], ride['PULocationID'], ride['DOLocationID'], ride['passenger_count'], ride['trip_distance'], ride['tip_amount'], ride['total_amount'])
    )
    count += 1
    if count % 100 == 0:
        print(f"Inserted {count} rows...")

consumer.close()
cur.close()
conn.close()
