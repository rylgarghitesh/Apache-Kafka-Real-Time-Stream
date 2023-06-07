package org.kafka.example;

import org.kafka.example.config.AppConfig;
import org.kafka.example.thread.Dispatcher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        System.out.println("Creating Kafka Producer1...");
        logger.info("Creating Kafka Producer...");

//        TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG()
//        ProducerConfig.BUFFER_MEMORY_CONFIG();

        Properties props = new Properties();
        try {
            InputStream inputStream = new FileInputStream(AppConfig.kafkaConfigFileLocation);
            props.load(inputStream);
//            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.applicationID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        Thread[] dispatchers = new Thread[AppConfig.eventFiles.length];

        logger.info("Starting Dispatcher threads...");
        System.out.println("Starting Dispatcher threads...");
        for(int i = 0; i < AppConfig.eventFiles.length; i++){
            dispatchers[i] = new Thread(new Dispatcher(producer,AppConfig.topicName,AppConfig.eventFiles[i]));
            dispatchers[i].start();
        }

        try {
            for (Thread t : dispatchers) t.join();
        }catch (InterruptedException e){
            logger.error("Main Thread Interrupted");
            System.out.println("Main Thread Interrupted");
        }finally {
            producer.close();
            logger.info("Finished Dispatcher Demo");
            System.out.println("Finished Dispatcher Demo");
        }
    }
}
