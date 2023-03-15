package homework;

import homework.data.Green_Trip;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.example.customserdes.CustomSerdes;

import java.util.Properties;

public class TripCounts {

    private final Properties props = new Properties();

    public TripCounts() {
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Variables.SERVER_HOST);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + org.example.Secrets.KAFKA_CLUSTER_KEY + "' password='" + Secrets.KAFKA_CLUSTER_SECRET + "';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kstream.joined.rides.pickuplocation.count.v3");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    }

    public static void main(String[] args) {
        var object = new TripCounts();
        object.countPLocationWindowed();
    }

    public Topology createTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        streamsBuilder.stream(Variables.INPUT_TOPIC, Consumed.with(Serdes.String(), CustomSerdes.getSerde(Green_Trip.class)))
                .groupByKey()
                .count()
                .toStream().mapValues(v -> v.toString() + "  trips")
                .to(Variables.OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }

    public void countPLocationWindowed() {
        var topology = createTopology();
        var kStreams = new KafkaStreams(topology, props);
        kStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kStreams::close));
    }
}
