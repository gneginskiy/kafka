package streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EventProducer {
    public static Logger log = LoggerFactory.getLogger("appl");

    public static void main(String[] args) throws Exception{
        try (var producer = new KafkaProducer<String, String>(producerConfig)){
            while (true){
                for (int i=0;i<1000;i++){
                    var key   = ""+i;
                    var value = "Event with the key #"+key;
                    log.info("produced message: "+key+" | "+value);
                    producer.send(new ProducerRecord<>("events", key, value));
                    Thread.sleep(100);
                }
            }
        }
    }

    private static final Map<String, Object> producerConfig = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091",
            ProducerConfig.ACKS_CONFIG, "all",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
}
