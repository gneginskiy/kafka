package transaction;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class TransactionalConsumer {
    public static Logger log = LoggerFactory.getLogger(TransactionalConsumer.class);

    public static void main(String[] args) {
        var topics = List.of("topic1", "topic2");
        try (var consumer = new KafkaConsumer<String, String>(createConsumerConfig())) {
            log.warn("Subscribe to {} with {}", topics, GROUP_ID);
            consumer.subscribe(topics);
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(10));
                for (var record : records) {
                    String topic = record.topic();
                    String message = record.value();
                    log.info("Received message from topic '{}' with value '{}'", topic, message);
                }
            }
        }
    }

    private static final String HOST = "localhost:9091";
    private static final String GROUP_ID = "java-transactions-app";

    private static Map<String, Object> createConsumerConfig() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                //thus we make sure that only confirmed msgs will be consumed
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID
        );
    }
}
