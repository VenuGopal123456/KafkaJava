import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SimpleKafkaProducer {
    public static void main(String[] args) {

        System.out.println("Helooo 123456789");
        String topicName = "test-topic";

        // Set Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka producer
        Producer<String, String> producer = new KafkaProducer<>(props);
        System.out.println("testing - sree");

        // Produce messages to the Kafka topic
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value);
            producer.send(record);
            System.out.println("Sent message: (" + key + ", " + value + ")");
        }

        // Close the producer
        producer.close();
    }
}
