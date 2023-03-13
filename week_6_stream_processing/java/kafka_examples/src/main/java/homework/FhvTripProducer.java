package homework;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import homework.data.Fhv_Trip;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;

import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class FhvTripProducer {
    private Properties props = new Properties();
    public FhvTripProducer() {
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Variables.SERVER_HOST);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='"+ Secrets.KAFKA_CLUSTER_KEY+"' password='"+Secrets.KAFKA_CLUSTER_SECRET+"';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
    }

    public List<Fhv_Trip> getTrips() throws IOException, CsvException {
        var ridesStream = this.getClass().getResource("/fhv_tripdata_2019-01.csv");
        var reader = new CSVReader(new FileReader(ridesStream.getFile()));
        reader.skip(1);
        return reader.readAll().stream().map(arr -> new Fhv_Trip(arr))
                .collect(Collectors.toList());

    }

    public void publishRides(List<Fhv_Trip> rides) throws ExecutionException, InterruptedException {
        KafkaProducer<String, Fhv_Trip> kafkaProducer = new KafkaProducer<String, Fhv_Trip>(props);
        for(Fhv_Trip ride: rides) {
            ride.pickup_datetime = LocalDateTime.now().minusMinutes(20);
            ride.dropoff_datetime = LocalDateTime.now();
            var record = kafkaProducer.send(new ProducerRecord<>("fhv_trips", String.valueOf(ride.PULocationID), ride), (metadata, exception) -> {
                if(exception != null) {
                    System.out.println(exception.getMessage());
                }
            });
            System.out.println(record.get().offset());
            System.out.println(ride.PULocationID);
            Thread.sleep(500);
        }
    }

    public static void main(String[] args) throws IOException, CsvException, ExecutionException, InterruptedException {
        var producer = new FhvTripProducer();
        var rides = producer.getTrips();
        producer.publishRides(rides);
    }
}