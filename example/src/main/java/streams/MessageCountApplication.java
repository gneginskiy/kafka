package streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

public class MessageCountApplication {

    private static final String HOST = "localhost:9091";

    public static void main(String[] args) {
        // Set up Kafka Streams configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "message-count-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, HOST);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // Create a StreamsBuilder object
        StreamsBuilder builder = new StreamsBuilder();

        // Create a KStream from the input topic
        KStream<String, String> input = builder.stream("events");

        // Group by key and count the messages within a 5-minute window
        KTable<Windowed<String>, Long> messageCounts = input
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5)))
                .count();

        // Output the results to an output topic
        messageCounts.toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .to("events-count", Produced.with(Serdes.String(), Serdes.Long()));

        var streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
