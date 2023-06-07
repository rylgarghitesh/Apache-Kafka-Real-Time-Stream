package org.kafka.example.config;

public class AppConfig {
    public final static String applicationID = "HelloProducer";
    public final static String bootstrapServers = "localhost:9092";
    public final static String topicName = "hello-producer-topic";
    public final static int numEvents = 1000000;
}
