package org.kafka.example.controller;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kafka.example.config.AppConfig;
import org.kafka.example.service.LoggingCallback;
import org.kafka.example.service.OddEvenPartitioner;

import java.sql.Timestamp;
import java.util.Properties;

public class SendMessage {

    private static final Logger logger = LogManager.getLogger();

    private Properties props;

    public SendMessage(Properties properties){
        this.props = properties;
        props.put(ProducerConfig.RETRIES_CONFIG, 0); //do not retry
        props.put(ProducerConfig.ACKS_CONFIG, "all"); //persisted to all brokers in ISR
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, OddEvenPartitioner.class); //Custom Partition
    }

    public void sendWithPartition() {
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

    public void sendAsyncMessage(){
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

    public void sendSyncMessage(){
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
}
