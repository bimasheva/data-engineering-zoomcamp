package homework.data;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Fhv_Trip {
    public String PULocationID;
    public LocalDateTime pickup_datetime;

    public Fhv_Trip(String[] arr) {
        PULocationID = arr[3];
        pickup_datetime = LocalDateTime.parse(arr[1], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
    public Fhv_Trip() {
    }

}
