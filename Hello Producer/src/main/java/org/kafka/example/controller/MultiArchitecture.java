package org.kafka.example.controller;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kafka.example.config.AppConfig;
import org.kafka.example.service.Dispatcher;

import java.util.Properties;

public class MultiArchitecture {

    private static final Logger logger = LogManager.getLogger();

    private Properties props;

    public MultiArchitecture(Properties properties) {
        this.props = properties;
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, AppConfig.transaction_id);
    }

    public void createMultiThread() {
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

    public void createTransactions(String order){
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
}
