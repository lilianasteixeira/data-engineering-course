# Cohort Homework

## Module 1:
### Question 1:
To answer this inside the python:3.13 docker container running the command: `pip --version`, so instead of run this command to install that version `docker run -it --rm --entrypoint=bash python:3.9.16`, I install `docker run -it --rm --entrypoint=bash python:3.13` and see the version of the pip

### Question 2:
For question 2 the correct answers from my tests are `postgres:5432` and `db:5432` because the container name is postgres and the container port is 5432, other correct option could is db:5432 because service name is bd and port still 5432, i tried localhost:5432 but i receive an error of network.

### Question 3: 
The correct option is 8007, for that I perform this query:
```
SELECT COUNT(*)
FROM green_tripdata
WHERE lpep_pickup_datetime BETWEEN '2025-11-01' AND '2025-12-01'
  AND trip_distance <= 1;
```

### Question 4:
The correct option is 2025-11-14 and I use the next query for that:
```
SELECT CAST(lpep_pickup_datetime AS DATE) AS lpep_pickup_date
FROM green_tripdata
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 1;
```

### Question 5:
The correct option is East Harlem North and I perform the next query:

```
SELECT pulocationid,
       zone,
       SUM(total_amount) AS total
FROM green_tripdata AS g
INNER JOIN taxi_zone_lookup AS z
    ON g.pulocationid = z.locationid
WHERE CAST(lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY pulocationid, zone
ORDER BY total DESC
LIMIT 1;
```

### Question 6:
I have doubts on this one, so I perform 2 different queries and the result was the same (Yorkville West):
```
SELECT zone,
       MAX(tip_amount) AS total
FROM green_tripdata AS g
INNER JOIN taxi_zone_lookup AS z
    ON g.dolocationid = z.locationid
WHERE lpep_pickup_datetime BETWEEN '2025-11-01' AND '2025-12-01'
  AND pulocationid IN (
        SELECT locationid
        FROM taxi_zone_lookup
        WHERE zone = 'East Harlem North'
  )
GROUP BY zone
ORDER BY total DESC
LIMIT 1;
```

```
SELECT z_drop.zone,
       MAX(g.tip_amount) AS total
FROM green_tripdata AS g
INNER JOIN taxi_zone_lookup AS z_pickup
    ON g.pulocationid = z_pickup.locationid
INNER JOIN taxi_zone_lookup AS z_drop
    ON g.dolocationid = z_drop.locationid
WHERE z_pickup.zone = 'East Harlem North'
  AND lpep_pickup_datetime BETWEEN '2025-11-01' AND '2025-12-01'
GROUP BY z_drop.zone
ORDER BY total DESC
LIMIT 1;
```

### Question 7: 
During the youtube class video to perform Downloading plugins and setting up backend we use `terraform init`, Generating and executing changes `terraform apply` and Removing all resources `terraform destroy`.

## Module 2:
### Question 1:
Extracting the file the size: 134.5MB doing the command `wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2020-12.csv.gz | gunzip > yellow_tripdata_2020-12.csv`

### Question 2:
When the pipeline is executed and the variables are passed to the code the file path change for this `green_tripdata_2020-04.csv`

### Question 3:
I execute this:
```
SELECT COUNT(1) 
FROM `peppy-caster-484310-c1.zoomcamp.yellow_tripdata`
WHERE filename LIKE 'yellow_tripdata_2020%'
```
Result: 24648499

### Question 4:
I execute this:
```
SELECT COUNT(1) 
FROM `peppy-caster-484310-c1.zoomcamp.green_tripdata`
WHERE filename LIKE 'green_tripdata_2020%'
```
Result: 1734051

### Question 5:
I perform this query:
```
SELECT COUNT(1) 
FROM `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_2021_03`
```
Result: 1925152

### Question 6:
The correct option is `Add a timezone property set to America/New_York in the Schedule trigger configuration`

## Module 3:
Steps to complete cohort: change code to inclode bucket and gcp credentials, next execute the code. Initially I include all 2024 months but the results doesn't match with cohort options.

### Question 1:
To answer this question, first I create the external table accessing my cloud storage parquet files (I use the same buckt from previous module):
```
CREATE OR REPLACE EXTERNAL TABLE `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_2024_ext`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-demo/*.parquet']
);
```
And next perform the count:
```
SELECT COUNT(*) 
FROM `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_2024_ext` 
```
The result: 20332093

### Question 2:
Performing this querys the result was 155,12 MB for the Materialized Table
```
SELECT COUNT(DISTINCT PULocationID) 
FROM `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_2024_ext` 
```

```
SELECT COUNT(DISTINCT PULocationID) 
FROM `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_2024` 
```

### Question 3:
Performed queries:
```
SELECT PULocationID 
FROM `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_2024` 
```
```
SELECT PULocationID, DOLocationID 
FROM `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_2024` 
```

### Question 4:
Performed query and result: 8333
```
SELECT COUNT(*)
FROM `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_2024` 
WHERE fare_amount = 0
```

### Question 5:
Performed queries:
```
CREATE OR REPLACE TABLE `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID
AS
SELECT * FROM `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_2024`;
```

### Question 6:
Performed queries and results: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table
```
SELECT DISTINCT(VendorID)
FROM `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_partitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```
```
SELECT DISTINCT(VendorID)
FROM `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_2024_ext`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

### Question 9:
Read 0B
```
SELECT COUNT(*) 
FROM `peppy-caster-484310-c1.zoomcamp.yellow_tripdata_2024_ext` 
```