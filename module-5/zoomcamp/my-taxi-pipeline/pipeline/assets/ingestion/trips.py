"""@bruin
name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

@bruin"""

import json
import os
from datetime import date, datetime, timedelta
from io import BytesIO
from typing import Iterable, List

import pandas as pd
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def _parse_date(env_var: str) -> date:
    value = os.getenv(env_var)
    if not value:
        raise ValueError(f"{env_var} is required for this asset.")
    return datetime.strptime(value, "%Y-%m-%d").date()


def _month_starts_in_range(start: date, end: date) -> Iterable[date]:
    """
    Generate the first day of each month that overlaps the [start, end) interval.
    The end date is treated as exclusive, which matches typical Bruin window semantics.
    """
    if start >= end:
        return []

    current = start.replace(day=1)
    last_day = end - timedelta(days=1)
    last = last_day.replace(day=1)

    months: List[date] = []
    while current <= last:
        months.append(current)
        year = current.year + (current.month // 12)
        month = 1 if current.month == 12 else current.month + 1
        current = date(year, month, 1)

    return months


def _get_taxi_types() -> List[str]:
    """
    Read the `taxi_types` pipeline variable from BRUIN_VARS.
    Falls back to ["yellow"] if not configured.
    """
    raw = os.getenv("BRUIN_VARS", "{}")
    try:
        vars_dict = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        vars_dict = {}

    taxi_types = vars_dict.get("taxi_types") or ["yellow"]
    if isinstance(taxi_types, str):
        taxi_types = [taxi_types]

    return [t.strip().lower() for t in taxi_types if t]


def _fetch_month_for_taxi_type(taxi_type: str, month_start: date, extracted_at: str) -> pd.DataFrame | None:
    """
    Fetch a single monthly parquet file for a given taxi_type and month.
    Returns a DataFrame or None if the file is not available (e.g., out-of-range dates).
    """
    filename = f"{taxi_type}_tripdata_{month_start.year}-{month_start.month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"

    response = requests.get(url, stream=True)
    if response.status_code != 200:
        # File may not exist for this month/taxi_type; skip quietly.
        return None

    buffer = BytesIO(response.content)
    df = pd.read_parquet(buffer)
    # Tag metadata for downstream use.
    df["taxi_type"] = taxi_type
    df["extracted_at"] = extracted_at
    return df


def materialize() -> pd.DataFrame:
    """
    Ingest NYC Taxi trip data into the `ingestion.trips` table.

    - Uses BRUIN_START_DATE / BRUIN_END_DATE to determine the date window.
    - Uses the `taxi_types` pipeline variable to decide which taxi types to ingest.
    - Downloads monthly parquet files from the TLC public S3 endpoint.
    - Returns a concatenated pandas DataFrame; Bruin handles the append materialization.
    """
    start_date = _parse_date("BRUIN_START_DATE")
    end_date = _parse_date("BRUIN_END_DATE")
    taxi_types = _get_taxi_types()

    extracted_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    dataframes: List[pd.DataFrame] = []

    for month_start in _month_starts_in_range(start_date, end_date):
        for taxi_type in taxi_types:
            df = _fetch_month_for_taxi_type(taxi_type, month_start, extracted_at)
            if df is not None and not df.empty:
                dataframes.append(df)

    if not dataframes:
        # No data found for this window; return an empty DataFrame.
        return pd.DataFrame()

    return pd.concat(dataframes, ignore_index=True)
