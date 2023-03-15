## Pre-requisite:

* Move the datasets fhv_tripdata_2019-01.csv.gz
and green_tripdata_2019-01.csv.gz
under /resources folder.
* Replace your cluster key and secret variables in [Secrets.java](Secrets.java) file
* Replace server host in [Variables.java](Variables.java) file 
* Create topics: "combined_trips" and "trips_count" in https://confluent.cloud/

## Running
* Run main [FhvTripProducer.java](FhvTripProducer.java)
* Run main [GreenTripProducer.java](GreenTripProducer.java)
* Run main [TripCounts.java](TripCounts.java)


## Result
![combined_trips.png](..%2F..%2Fresources%2Fcombined_trips.png)

![trips_count.png](..%2F..%2Fresources%2Ftrips_count.png)


