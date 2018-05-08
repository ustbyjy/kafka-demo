package com.ascend.spring;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

public class SpringKafkaConsumerListener implements MessageListener<String, String> {
    public void onMessage(ConsumerRecord<String, String> data) {
        if (null != data) {
            System.out.println("消费者线程:" + Thread.currentThread().getName() + ",消息来自Kafka,主题[" + data.topic() +
                    "],分区[" + data.partition() + "],委托时间[" + data.timestamp() + "],消息内容如下:");
            System.out.println(data.value());
        }
    }
}
