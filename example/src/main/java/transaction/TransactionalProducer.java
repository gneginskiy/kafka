package transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class TransactionalProducer {
    public static Logger log = LoggerFactory.getLogger("appl");

    public static void main(String[] args) {
        var producer = new KafkaProducer<String, String>(createProducerConfig(config -> {
            config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");
        }));
        producer.initTransactions();

        try {
            // Start the first transaction
            producer.beginTransaction();
            for (int i = 0; i < 5; i++) {
                var key   = ""+i;
                var value = "Message from confirmed transaction #"+i;
                producer.send(new ProducerRecord<>("topic1", key, value));
                producer.send(new ProducerRecord<>("topic2", key, value));
            }
            // Commit the first transaction
            producer.commitTransaction();

            // Start the second transaction
            producer.beginTransaction();
            for (int i = 0; i < 20000; i++) {
                var key   = ""+i;
                var value = "Message from aborted transaction #"+i;
                producer.send(new ProducerRecord<>("topic1", key, value));
                producer.send(new ProducerRecord<>("topic2", key, value));
            }
            // Aborting the second transaction
            producer.abortTransaction();
        } catch (ProducerFencedException e) {
            producer.close();
            log.error("Producer got fenced: {}", e.getMessage());
        } finally {
            producer.close();
        }
    }

    private static final String HOST = "localhost:9091";

    private static final Map<String, Object> producerConfig = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST,
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

    private static Map<String, Object> createProducerConfig(Consumer<Map<String, Object>> builder) {
        var map = new HashMap<>(producerConfig);
        builder.accept(map);
        return map;
    }
}
