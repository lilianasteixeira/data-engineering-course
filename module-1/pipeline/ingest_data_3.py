import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm

# --- DB config (adapt as needed) ---
POSTGRES_USER = "root"
POSTGRES_PASSWORD = "root"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DB = "ny_taxi"

engine = create_engine(
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@"
    f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# --- Source Parquet file (Green Taxi 2025-11) ---
PARQUET_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet"

TABLE_NAME = "green_tripdata"
CHUNK_SIZE = 100_000  # adjust if needed


def main():
    # Read full Parquet file into a DataFrame
    # (if memory is tight, switch to pyarrow dataset & batch iteration)
    df = pd.read_parquet(PARQUET_URL)  # uses pyarrow/fastparquet under the hood

    # Optional: normalize column names to snake_case / lowercase
    df.columns = [c.lower() for c in df.columns]

    # First: create/replace table with the schema only
    df.head(0).to_sql(
        name=TABLE_NAME,
        con=engine,
        if_exists="replace",
        index=False,
    )

    # Then: append data in chunks
    n = len(df)
    for start in tqdm(range(0, n, CHUNK_SIZE)):
        end = min(start + CHUNK_SIZE, n)
        chunk = df.iloc[start:end]
        chunk.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists="append",
            index=False,
        )


if __name__ == "__main__":
    main()
