package com.ascend.demo;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class KafkaSimpleConsumer {
    private static final String BROKER_LIST = "10.236.40.159:9092,10.236.40.159:9093,10.236.40.159:9094";
    private static final int TIME_OUT = 60 * 1000;
    private static final int BUFFER_SIZE = 1024 * 1024;
    private static final int FETCH_SIZE = 100000;
    private static final int MAX_ERROR_NUM = 3;

    private PartitionMetadata fetchPartitionMetadata(List<String> brokerList, String topic, int partitionId) {
        SimpleConsumer consumer = null;
        TopicMetadataRequest metadataRequest = null;
        TopicMetadataResponse metadataResponse = null;
        List<TopicMetadata> topicsMetadata = null;
        try {
            for (String broker : brokerList) {
                String[] hostAndPort = broker.split(":");
                consumer = new SimpleConsumer(hostAndPort[0], Integer.parseInt(hostAndPort[1]), TIME_OUT, BUFFER_SIZE, "fetch-metadata");
                metadataRequest = new TopicMetadataRequest(Arrays.asList(topic));
                try {
                    metadataResponse = consumer.send(metadataRequest);
                } catch (Exception e) {
                    log.error("Send topicMetadataRequest occurs exception", e);
                    continue;
                }
                topicsMetadata = metadataResponse.topicsMetadata();
                for (TopicMetadata metadata : topicsMetadata) {
                    for (PartitionMetadata item : metadata.partitionsMetadata()) {
                        if (item.partitionId() != partitionId) {
                            continue;
                        } else {
                            return item;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Fetch PartitionMetadata occurs exception", e);
        } finally {
            if (null != consumer) {
                consumer.close();
            }
        }
        return null;
    }

    private long getLastOffset(SimpleConsumer consumer, String topic, int partition, long beginTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(beginTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            log.error("Fetch last offset occurs exception" + response.errorCode(topic, partition));
            return -1;
        }
        long[] offsets = response.offsets(topic, partition);
        if (null == offsets || offsets.length == 0) {
            log.error("Fetch last offset occurs error,offsets is null");
            return -1;
        }
        return offsets[0];
    }

    public void consume(List<String> brokerList, String topic, int partitionId) {
        SimpleConsumer consumer = null;
        try {
            PartitionMetadata metadata = fetchPartitionMetadata(brokerList, topic, partitionId);
            if (metadata == null) {
                log.error("Can't find metadata info");
                return;
            }
            if (metadata.leader() == null) {
                log.error("Can't find the partition:" + partitionId + " 's leader");
            }
            String leadBroker = metadata.leader().host();
            int leadBrokerPort = metadata.leader().port();
            String clientId = "client-" + topic + "-" + partitionId;
            consumer = new SimpleConsumer(leadBroker, leadBrokerPort, TIME_OUT, BUFFER_SIZE, clientId);
            long lastOffset = getLastOffset(consumer, topic, partitionId, kafka.api.OffsetRequest.EarliestTime(), clientId);
            int errorNum = 0;
            kafka.api.FetchRequest fetchRequest = null;
            FetchResponse fetchResponse = null;
            while (lastOffset > -1) {
                if (consumer == null) {
                    consumer = new SimpleConsumer(leadBroker, leadBrokerPort, TIME_OUT, BUFFER_SIZE, clientId);
                }
                fetchRequest = new FetchRequestBuilder().clientId(clientId).addFetch(topic, partitionId, lastOffset, FETCH_SIZE).build();
                fetchResponse = consumer.fetch(fetchRequest);
                if (fetchResponse.hasError()) {
                    errorNum++;
                    if (errorNum > MAX_ERROR_NUM) {
                        break;
                    }
                    short errorCode = fetchResponse.errorCode(topic, partitionId);
                    if (ErrorMapping.OffsetOutOfRangeCode() == errorCode) {
                        lastOffset = getLastOffset(consumer, topic, partitionId, kafka.api.OffsetRequest.LatestTime(), clientId);
                        continue;
                    } else if (ErrorMapping.OffsetsLoadInProgressCode() == errorCode) {
                        Thread.sleep(30000);
                        continue;
                    } else {
                        consumer.close();
                        consumer = null;
                        continue;
                    }
                } else {
                    errorNum = 0;
                    long fetchNum = 0;
                    for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partitionId)) {
                        long currentOffset = messageAndOffset.offset();
                        if (currentOffset < lastOffset) {
                            log.error("Fetch an old offset:" + currentOffset + ", expect the offset is greater than " + lastOffset);
                            continue;
                        }
                        lastOffset = messageAndOffset.nextOffset();
                        ByteBuffer payload = messageAndOffset.message().payload();
                        byte[] bytes = new byte[payload.limit()];
                        payload.get(bytes);
                        log.info("message:" + (new String(bytes, "UTF-8")) + ", offset:" + messageAndOffset.offset());
                        fetchNum++;
                    }
                    if (fetchNum == 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException ie) {
                        }
                    }
                }

            }
        } catch (Exception e) {
            log.error("Consume message occurs exception", e);
        } finally {
            if (null != consumer) {
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {
        KafkaSimpleConsumer consumer = new KafkaSimpleConsumer();
        consumer.consume(Arrays.asList(BROKER_LIST.split(",")), "stock-quotation", 5);
    }

}
