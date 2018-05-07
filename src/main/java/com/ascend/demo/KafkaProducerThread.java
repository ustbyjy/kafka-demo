package com.ascend.demo;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

@Slf4j
public class KafkaProducerThread implements Runnable {
    private KafkaProducer<String, String> producer = null;
    private ProducerRecord<String, String> record = null;

    public KafkaProducerThread(KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
        this.producer = producer;
        this.record = record;
    }

    public void run() {
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (null != exception) {
                    log.error("Send message occurs exception", exception);
                }
                if (null != metadata) {
                    log.info("offset:{}, partition:{}", metadata.offset(), metadata.partition());
                }
            }
        });
    }
}
