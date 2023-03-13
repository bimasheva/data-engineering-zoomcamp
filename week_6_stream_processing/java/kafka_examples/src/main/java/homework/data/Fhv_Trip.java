package homework.data;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Fhv_Trip {
    public Fhv_Trip(String[] arr) {
        dispatching_base_num = arr[0];
        pickup_datetime = LocalDateTime.parse(arr[1], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        dropoff_datetime = LocalDateTime.parse(arr[2], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        PULocationID = arr[3];
        DOLocationID = arr[4];
        SR_Flag = arr[5];
        Affiliated_base_number = arr[6];
    }
    public Fhv_Trip(){}
    public String dispatching_base_num;
    public LocalDateTime pickup_datetime;
    public LocalDateTime dropoff_datetime;

    public String PULocationID;
    public String DOLocationID;
    public String SR_Flag;
    public String Affiliated_base_number;

}
