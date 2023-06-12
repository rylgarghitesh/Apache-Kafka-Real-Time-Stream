package org.kafka.example;

import org.kafka.example.config.AppConfig;
import org.kafka.example.controller.MultiArchitecture;
import org.kafka.example.controller.SendMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    static Properties setProp(){
        Properties props = new Properties();
        try {
            InputStream inputStream = new FileInputStream(AppConfig.kafkaConfigFileLocation);
            props.load(inputStream);
            //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.applicationID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
         } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return props;
    }


    public static void main(String[] args) {
        System.out.println("Starting Kafka");
        logger.info("Starting Kafka");

        logger.trace("Creating Kafka Producer...");
        System.out.println("Creating Kafka Producer...");
        Properties props = setProp();

//        TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG();
//        ProducerConfig.BUFFER_MEMORY_CONFIG();

        MultiArchitecture multiArchitecture = new MultiArchitecture(props);
        logger.trace("Creating Multi Thread Producer...");
        System.out.println("Creating Multi Thread Producer...");
        multiArchitecture.createMultiThread();

        logger.trace("Creating Multi Transactional Producer...");
        System.out.println("Creating Multi Transactional Producer...");
        multiArchitecture.createTransactions( "first");
        multiArchitecture.createTransactions("second");


        SendMessage sendMessage = new SendMessage(props);
        logger.trace("Sending sync messages, close 1 broker...");
        System.out.println("Sending sync messages, close 1 broker...");
        sendMessage.sendSyncMessage();

        logger.trace("Sending async messages, close 1 broker...");
        System.out.println("Sending async messages, close 1 broker...");
        sendMessage.sendAsyncMessage();

        logger.trace("Sending with custom partition messages...");
        System.out.println("Sending with custom partition messages...");
        sendMessage.sendWithPartition();

        logger.info("Finished Kafka");
        System.out.println("Finished Kafka");
    }
}
