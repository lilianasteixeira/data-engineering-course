import pandas as pd
from sqlalchemy import create_engine

# Adjust to your environment
POSTGRES_USER = "root"
POSTGRES_PASSWORD = "root"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DB = "ny_taxi"

engine = create_engine(
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# NYC TLC taxi zone lookup CSV
TAXI_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

# Optional: explicit schema to avoid surprises
dtype = {
    "LocationID": "Int64",
    "Borough": "string",
    "Zone": "string",
    "service_zone": "string",
}

# Read whole file (small) — or add chunksize if you prefer
df = pd.read_csv(TAXI_ZONE_URL, dtype=dtype)

# Normalize column names if you want snake_case
df.columns = [c.lower() for c in df.columns]

# Write to Postgres
table_name = "taxi_zone_lookup"

# First write: create/replace table
df.head(0).to_sql(name=table_name, con=engine, if_exists="replace", index=False)

# Then append data
df.to_sql(name=table_name, con=engine, if_exists="append", index=False)
