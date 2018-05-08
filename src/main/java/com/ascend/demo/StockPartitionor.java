package com.ascend.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

@Slf4j
public class StockPartitionor implements Partitioner {

    private static final Integer PARTITIONS = 6;

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (null == key) {
            return 0;
        }
        String stockCode = String.valueOf(key);
        try {
            int partitionId = Integer.valueOf(stockCode.substring(stockCode.length() - 2)) % PARTITIONS;
            return partitionId;
        } catch (Exception e) {
            log.error("Parse message key occurs exception, key:" + stockCode, e);
            return 0;
        }
    }

    public void close() {
    }

    public void configure(Map<String, ?> configs) {
    }
}
