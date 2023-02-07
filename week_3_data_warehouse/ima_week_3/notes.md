#### Question 1
```
SELECT count(*) from `dtc-de-course-374916.trips_data_all.fhv_tripdata_internal`;
```

#### Question 2

```
### External table
SELECT count(*) from `dtc-de-course-374916.trips_data_all.fhv_data`;

### Internal table
SELECT count(*) from `dtc-de-course-374916.trips_data_all.fhv_tripdata_internal`;
```

#### Question 3
```
SELECT
  COUNT(*)
FROM
  `dtc-de-course-374916.trips_data_all.fhv_tripdata_internal`
WHERE
  PUlocationID IS NULL
  AND DOlocationID IS NULL;
```

#### Question 4
```
CREATE TABLE `dtc-de-course-374916.trips_data_all.fhv_tripdata_partitioned` 
PARTITION BY DATE(pickup_datetime)
 CLUSTER BY affiliated_base_number
AS
SELECT *  FROM `dtc-de-course-374916.trips_data_all.fhv_tripdata_internal`
```

#### Question 5
```
SELECT COUNT(DISTINCT affiliated_base_number)
FROM `dtc-de-course-374916.trips_data_all.fhv_tripdata_internal` 
WHERE DATE(pickup_datetime) BETWEEN DATE("2019-03-01") AND DATE("2019-03-31")

SELECT COUNT(DISTINCT affiliated_base_number)
FROM `dtc-de-course-374916.trips_data_all.fhv_tripdata_partitioned` 
WHERE DATE(pickup_datetime) BETWEEN DATE("2019-03-01") AND DATE("2019-03-31")
```