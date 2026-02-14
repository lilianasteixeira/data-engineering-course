#!/usr/bin/env python3
import duckdb

db_path = "/Users/lilianateixeira/Desktop/Portefolio/data_engeneering_course/data-engineering-course/module-4/taxi_rides_ny/taxi_rides_ny.duckdb"

conn = duckdb.connect(db_path)

try:
    print("Creating raw schema...")
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    conn.execute("DROP TABLE IF EXISTS raw.fhv_tripdata")
    
    # Install and load httpfs extension
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    
    print("Loading FHV 2019 data from GitHub (12 months)...\n")
    
    # Base URL for GitHub release
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv"
    year = 2019
    months = [f"{i:02d}" for i in range(1, 13)]
    
    table_created = False
    
    # Load all 12 months of 2019
    for i, month in enumerate(months, 1):
        url = f"{base_url}/fhv_tripdata_{year}-{month}.csv.gz"
        print(f"[{i:2d}/12] Loading {year}-{month}...", end=" ")
        
        try:
            if not table_created:
                # Create table with first month
                conn.execute(f"CREATE TABLE raw.fhv_tripdata AS SELECT * FROM read_csv_auto('{url}')")
                table_created = True
                print("✓")
            else:
                # Insert subsequent months
                conn.execute(f"INSERT INTO raw.fhv_tripdata SELECT * FROM read_csv_auto('{url}')")
                print("✓")
        except Exception as e:
            print(f"✗ Error: {str(e)[:60]}")
            continue
    
    result = conn.execute("SELECT COUNT(*) as cnt FROM raw.fhv_tripdata").fetchall()
    count = result[0][0]
    print(f"\n✓ Successfully loaded {count:,} FHV 2019 records into raw.fhv_tripdata")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    conn.close()
