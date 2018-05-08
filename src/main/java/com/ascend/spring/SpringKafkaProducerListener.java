package com.ascend.spring;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;

public class SpringKafkaProducerListener implements ProducerListener<String, String> {

    public void onSuccess(String topic, Integer partition, String key, String value, RecordMetadata recordMetadata) {
        System.out.println("委托成功:主题[" + topic + "],分区[" + recordMetadata.partition() + "],委托时间[" + recordMetadata.timestamp() + "],委托信息如下:");
        System.out.println(value);
    }

    public void onError(String topic, Integer partition, String key, String value, Exception exception) {

    }

    public boolean isInterestedInSuccess() {
        return true;
    }
}
