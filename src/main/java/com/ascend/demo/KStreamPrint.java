package com.ascend.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;

public class KStreamPrint {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KStream-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "vhost1:9092,vhost1:9093,vhost1:9094");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLine = builder.stream("streams-foo");
        textLine.print();
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        Thread.sleep(5000L);
        streams.close();
    }

}
