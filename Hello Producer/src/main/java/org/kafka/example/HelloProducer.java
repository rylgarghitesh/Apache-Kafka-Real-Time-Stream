package org.kafka.example;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.kafka.example.config.AppConfig;
import org.kafka.example.service.Dispatcher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kafka.example.service.LoggingCallback;
import org.kafka.example.service.OddEvenPartitioner;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.Properties;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    static Properties setProp(){
        Properties props = new Properties();
        try {
            InputStream inputStream = new FileInputStream(AppConfig.kafkaConfigFileLocation);
            props.load(inputStream); //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.bootstrapServers);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfig.applicationID);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
         } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return props;
    }

    static void createMultiThread(Properties props) {
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

    static void createTransactions(Properties props, String order){
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();

        logger.info("Starting " + order + " Transaction...");
        producer.beginTransaction();
        try {
            for (int i = 1; i <= AppConfig.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfig.topicName1, i, "Simple Message-T" + order + "-" +i));
                producer.send(new ProducerRecord<>(AppConfig.topicName2, i, "Simple Message-T" + order + "-" +i));
            }
            if(order.equals("first")) {
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

    static void createMultiTransactional(Properties props) {
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, AppConfig.transaction_id);
        createTransactions(props, "first");
        createTransactions(props, "second");
    }

    static void sendSyncMessage(Properties props){
        props.put(ProducerConfig.RETRIES_CONFIG, 0); //do not retry
        props.put(ProducerConfig.ACKS_CONFIG, "all"); //persisted to all brokers in ISR
        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= AppConfig.numEvents; i++) {
                Thread.sleep(1000);
                RecordMetadata metadata = producer.send(new ProducerRecord<>(AppConfig.topicName, i, "Simple Message-" + i)).get();
                logger.info("Message " + i + " persisted with offset " + metadata.offset()
                        + " and timestamp on " + new Timestamp(metadata.timestamp()));

                System.out.println("Message " + i + " persisted with offset " + metadata.offset()
                        + " and timestamp on " + new Timestamp(metadata.timestamp()));
            }
        } catch (Exception e) {
            logger.info("Can't send message - Received exception \n" + e.getMessage());
            System.out.println("Can't send message - Received exception \n" + e.getMessage());
        }
    }

    static void sendAsyncMessage(Properties props){
        props.put(ProducerConfig.RETRIES_CONFIG, 0); //do not retry
        props.put(ProducerConfig.ACKS_CONFIG, "all"); //persisted to all brokers in ISR
        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= AppConfig.numEvents; i++) {
                Thread.sleep(1000);
                String message = "Simple Message-" + i;
                producer.send(new ProducerRecord<>(AppConfig.topicName, i, message),
                        new LoggingCallback(message));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static void sendWithPartition(Properties props) {
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OddEvenPartitioner.class); //Custom Partition
        try (KafkaProducer<Integer, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= AppConfig.numEvents; i++) {
                Thread.sleep(1000);
                RecordMetadata metadata = producer.send(new ProducerRecord<>(AppConfig.topicName, i, "Simple Message-" + i)).get();
                logger.info("Message " + i + " persisted with offset " + metadata.offset()
                        + " in partition " + metadata.partition());
                System.out.println("Message " + i + " persisted with offset " + metadata.offset()
                        + " in partition " + metadata.partition());
            }
        } catch (Exception e) {
            logger.info("Can't send message - Received exception \n" + e.getMessage());
            System.out.println("Can't send message - Received exception \n" + e.getMessage());
        }
    }

    public static void main(String[] args) {
        System.out.println("Starting Kafka");
        logger.info("Starting Kafka");

        logger.trace("Creating Kafka Producer...");
        System.out.println("Creating Kafka Producer...");
        Properties props = setProp();

//        TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG();
//        ProducerConfig.BUFFER_MEMORY_CONFIG();

        logger.trace("Creating Multi Thread Producer...");
        System.out.println("Creating Multi Thread Producer...");
        createMultiThread(props);

        logger.trace("Creating Multi Transactional Producer...");
        System.out.println("Creating Multi Transactional Producer...");
        createMultiTransactional(props);

        logger.trace("Sending sync messages, close 1 broker...");
        System.out.println("Sending sync messages, close 1 broker...");
        sendSyncMessage(props);

        logger.trace("Sending async messages, close 1 broker...");
        System.out.println("Sending async messages, close 1 broker...");
        sendAsyncMessage(props);

        logger.trace("Sending with custom partition messages...");
        System.out.println("Sending with custom partition messages...");
        sendWithPartition(props);

        logger.info("Finished Kafka");
        System.out.println("Finished Kafka");
    }
}
