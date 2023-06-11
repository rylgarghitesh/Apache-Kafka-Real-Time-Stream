package org.kafka.example.service;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.record.InvalidRecordException;

import java.util.Map;

public class OddEvenPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if ((topic == null) || (!(key instanceof Integer)))
            throw new InvalidRecordException("Topic Key must have a valid Integer value.");

        if (cluster.partitionsForTopic(topic).size() != 1)
            throw new InvalidTopicException("Topic must have exactly one partitions");

        return (Integer) key % 1;
    }

    @Override
    public void close() { }

    @Override
    public void configure(Map<String, ?> map) { }
}
