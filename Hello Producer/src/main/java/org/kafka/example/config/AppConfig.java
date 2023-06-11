package org.kafka.example.config;

public class AppConfig {
    public final static String applicationID = "HelloProducer";

    public  final static String topicName = "hello-producer-3";

    public final static String topicName1 = "hello-producer-4";

    public final static String topicName2 = "hello-producer-5";

    public final static String transaction_id = "Hello-Producer-Trans";
    
    public final static String kafkaConfigFileLocation = "Hello Producer/src/main/resources/kafka.properties";

    public final static int numEvents = 100;

    public final static String[] eventFiles = {"data/NSE05NOV2018BHAV.csv","data/NSE06NOV2018BHAV.csv"};
}
