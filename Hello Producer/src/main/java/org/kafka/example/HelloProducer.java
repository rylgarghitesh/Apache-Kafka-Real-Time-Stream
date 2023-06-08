package org.kafka.example;

import org.apache.kafka.clients.producer.ProducerRecord;
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

    Properties setProp(){
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
        return props;
    }

    void createMultiThread() {
        Properties props = setProp();
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
        } catch (InterruptedException e){
            logger.error("Main Thread Interrupted");
            System.out.println("Main Thread Interrupted");
        }finally {
            producer.close();
        }
    }

    public static void main(String[] args) {

        System.out.println("Starting Kafka");
        logger.info("Starting Kafka");

//        TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG()
//        ProducerConfig.BUFFER_MEMORY_CONFIG();

//        new HelloProducer().CreateMultiThread();
        new HelloProducer().createMultiTransactional();

        logger.info("Finished Kafka");
        System.out.println("Finished Kafka");

    }

    void createTransactions(Properties props, String order){
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();

        logger.info("Starting " + order + " Transaction...");
        producer.beginTransaction();
        try {
            for (int i = 1; i <= AppConfig.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfig.topicName1, i, "Simple Message-T" + order + "-" +i));
                producer.send(new ProducerRecord<>(AppConfig.topicName2, i, "Simple Message-T" + order + "-" +i));
            }
            if(order == "first") {
                logger.info("Committing " + order + " First Transaction.");
                producer.commitTransaction();
            } else {
                logger.info("Aborting " + order + " Transaction.");
                producer.abortTransaction();
            }
        }catch (Exception e){
            logger.error("Exception in " + order + " Transaction. Aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        } finally {
            producer.close();
        }
    }

    private void createMultiTransactional() {
        Properties props = setProp();
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, AppConfig.transaction_id);

        createTransactions(props, "first");
        createTransactions(props, "second");
    }
}
