/* @bruin
name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table

custom_checks:
  - name: row_count_positive
    description: Ensure the staging table has rows for the window
    query: SELECT count(*) > 0 FROM staging.trips
    value: 1

@bruin */

WITH source AS (

    SELECT *
    FROM ingestion.trips
    WHERE tpep_pickup_datetime >= '{{ start_datetime }}'
      AND tpep_pickup_datetime < '{{ end_datetime }}'

),

deduplicated AS (

    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY vendor_id,
                         tpep_pickup_datetime,
                         tpep_dropoff_datetime,
                         passenger_count,
                         trip_distance
            ORDER BY tpep_pickup_datetime
        ) AS row_num
    FROM source

),

cleaned AS (

    SELECT
        vendor_id,
        tpep_pickup_datetime AS pickup_datetime,
        tpep_dropoff_datetime AS dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecode_id AS rate_code_id,
        store_and_fwd_flag,
        pu_location_id,
        do_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount

    FROM deduplicated
    WHERE row_num = 1
      AND vendor_id IS NOT NULL
      AND tpep_pickup_datetime IS NOT NULL
      AND tpep_dropoff_datetime IS NOT NULL
      AND trip_distance >= 0
      AND total_amount >= 0

)

SELECT *
FROM cleaned;
