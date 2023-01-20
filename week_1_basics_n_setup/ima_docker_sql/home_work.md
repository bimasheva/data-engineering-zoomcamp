RUN Docker with postgresql
```dockerfile
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

Question 1: 
```
--iidfile
```

Question 2: 
```bash
docker run -it python:3.9 bash 
pip list
```

Question 3:
```postgres-sql
SELECT count(1) 
 FROM green_trip 
 WHERE lpep_pickup_datetime>='2019-01-15 00:00:00' 
    AND lpep_pickup_datetime<'2019-01-16 00:00:00'
    AND lpep_dropoff_datetime>='2019-01-15 00:00:00'
    AND lpep_dropoff_datetime<'2019-01-16 00:00:00';
```

Question 4:
```postgres-sql
SELECT DATE(lpep_pickup_datetime) AS dt
    , trip_distance AS dist 
FROM green_trip 
ORDER BY dist DESC
LIMIT 1;
```

Question 5:
```postgres-sql
SELECT passenger_count, count(1) AS cnt
FROM green_trip 
WHERE lpep_pickup_datetime>='2019-01-01 00:00:00' 
    AND lpep_pickup_datetime<'2019-01-02 00:00:00' 
    AND passenger_count IN (2,3)
GROUP BY passenger_count;
```

Question 6:
```postgres-sql
SELECT a."Zone" 
FROM taxi_zone a 
WHERE a."LocationID" = (SELECT t."DOLocationID"
                         FROM green_trip t
                         WHERE t."PULocationID"= (SELECT a."LocationID" 
                                                    FROM taxi_zone a 
                                                   WHERE a."Zone" = 'Astoria')
                        ORDER BY tip_amount DESC limit 1);
```
