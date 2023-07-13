package streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MessageCountConsumer {
    public static Logger log = LoggerFactory.getLogger("appl");

    public static void main(String[] args) {
        // Set up Kafka consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "message-count-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());

        // Create a Kafka consumer
        KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(props);

        // Subscribe to the output topic
        consumer.subscribe(Collections.singletonList("events-count"));

        // Poll for new records and print the results
        while (true) {
            ConsumerRecords<String, Long> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Long> record : records) {
                String msg = "Key: " + record.key() + ", Count: " + record.value();
                System.out.println(msg);
                log.info(msg);
            }
        }
    }
}

