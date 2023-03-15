package homework.data;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Green_Trip {
    public long PULocationID;
    public LocalDateTime pickup_datetime;

    public Green_Trip(String[] arr) {
        PULocationID = Long.parseLong(arr[5]);
        pickup_datetime = LocalDateTime.parse(arr[1], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
    public Green_Trip() {
    }

}
