package transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

public class TransactionalProducer {
    public static void main(String[] args) {
        try (var producer = new KafkaProducer<String, String>(createProducerConfig())){
            producer.initTransactions();
            // Start the first transaction
            producer.beginTransaction();
            for (int i = 0; i < 5; i++) {
                var key = "" + i;
                var value = "Message from confirmed transaction #" + i;
                producer.send(new ProducerRecord<>("topic1", key, value));
                producer.send(new ProducerRecord<>("topic2", key, value));
            }
            // Commit the first transaction
            producer.commitTransaction();
            // Start the second transaction
            producer.beginTransaction();
            for (int i = 0; i < 20000; i++) {
                var key = "" + i;
                var value = "Message from aborted transaction #" + i;
                producer.send(new ProducerRecord<>("topic1", key, value));
                producer.send(new ProducerRecord<>("topic2", key, value));
            }
            // Aborting the second transaction
            producer.abortTransaction();
        }
    }

    private static final String HOST = "localhost:9091";

    private static Map<String, Object> createProducerConfig() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, HOST,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id"
        );
    }
}
