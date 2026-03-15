import json
from dataclasses import dataclass


@dataclass
class Ride:
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float


def ride_from_row(row):
    return Ride(
        lpep_pickup_datetime=row['lpep_pickup_datetime'].isoformat(),
        lpep_dropoff_datetime=row['lpep_dropoff_datetime'].isoformat(),
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        passenger_count=int(row['passenger_count']),
        trip_distance=float(row['trip_distance']),
        tip_amount=float(row['tip_amount']),
        total_amount=float(row['total_amount']),
    )


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
