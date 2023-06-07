package org.kafka.example;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.protocol.Message;
import org.kafka.example.config.AppConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        System.out.println("Creating Kafka Producer1...");
        logger.info("Creating Kafka Producer...");

//        TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG()
//        ProducerConfig.BUFFER_MEMORY_CONFIG();

        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        logger.info("Start sending messages...");
        for (int i = 1; i <= AppConfig.numEvents; i++) {
            producer.send(new ProducerRecord<>(AppConfig.topicName, i, "Simple Message-" + i));
            System.out.println("Creating Kafka Producer1...");
        }

        logger.info("Finished - Closing Kafka Producer.");
        producer.close();

    }
}
