/* @bruin
name: reports.trips_report
type: duckdb.sql
depends:
  - staging.trips

materialization:
  type: table

columns:
  - name: trip_date
    type: date
    description: Date of the trip (based on pickup_datetime)
    primary_key: true
  - name: taxi_type
    type: varchar
    description: Type of taxi (Green or Yellow)
    primary_key: true
  - name: payment_type_name
    type: varchar
    description: Payment method name
    primary_key: true
  - name: trip_count
    type: bigint
    description: Total Number of trips
    checks:
      - name: positive
  - name: total_passengers
    type: bigint
    description: Total number of passengers
    checks:
      - name: non_negative
  - name: total_distance
    type: double
    description: Total distance traveled in miles
    checks:
      - name: non_negative
  - name: total_fare
    type: double
    description: Total fare amount
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: double
    description: Average trip distance in miles
  - name: avg_passengers
    type: double
    description: Average number of passengers

custom_checks:
  - name: row_count_positive
    description: Ensure the report is not empty
    query: SELECT count(*) > 0 FROM reports.trips_report
    value: 1

@bruin */


SELECT
  CAST(pickup_datetime AS DATE) AS trip_date,
  'Yellow' AS taxi_type,  -- hardcoded because staging.trips has no taxi_type
  CASE payment_type
    WHEN 1 THEN 'Credit card'
    WHEN 2 THEN 'Cash'
    WHEN 3 THEN 'No charge'
    WHEN 4 THEN 'Dispute'
    WHEN 5 THEN 'Unknown'
    WHEN 6 THEN 'Voided trip'
    ELSE 'Other'
  END AS payment_type_name,

  -- Count metrics
  COUNT(*) AS trip_count,
  SUM(COALESCE(passenger_count, 0)) AS total_passengers,

  -- Distance metrics
  SUM(COALESCE(trip_distance, 0)) AS total_distance,

  -- Revenue metrics
  SUM(COALESCE(fare_amount, 0)) AS total_fare,
  SUM(COALESCE(tip_amount, 0)) AS total_tips,
  SUM(COALESCE(total_amount, 0)) AS total_revenue,

  -- Average metrics
  AVG(COALESCE(fare_amount, 0)) AS avg_fare,
  AVG(COALESCE(trip_distance, 0)) AS avg_trip_distance,
  AVG(COALESCE(passenger_count, 0)) AS avg_passengers

FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
  CAST(pickup_datetime AS DATE),
  taxi_type,
  payment_type_name;

