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
### Question 2:
### Question 3:
### Question 4: