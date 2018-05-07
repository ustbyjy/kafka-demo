package com.ascend.demo;

import com.ascend.dto.StockQuotationInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.text.DecimalFormat;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
public class QuotationProducer {

    /**
     * 设置实例生产消息的总数
     */
    private static final int MSG_SIZE = 100;
    /**
     * 主题名称
     */
    private static final String TOPIC = "stock-quotation";
    /**
     * Kafka集群
     */
    private static final String BROKER_LIST = "10.236.40.159:9092,10.236.40.159:9093,10.236.40.159:9094";

    private static KafkaProducer<String, String> producer = null;

    static {
        // 1.构造用于实例化KafkaProducer的Properties信息
        Properties configs = initConfig();
        // 2.初始化一个KafkaProducer
        producer = new KafkaProducer<String, String>(configs);
    }

    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

    private static StockQuotationInfo createQuotationInfo() {
        StockQuotationInfo quotationInfo = new StockQuotationInfo();
        Random r = new Random();
        Integer stockCode = 600100 + r.nextInt(10);
        float random = (float) Math.random();
        if (random / 2 < 0.5) {
            random = -random;
        }
        DecimalFormat decimalFormat = new DecimalFormat(".00");
        quotationInfo.setCurrentPrice(Float.valueOf(decimalFormat.format(11 + random)));
        quotationInfo.setPreClosePrice(11.80f);
        quotationInfo.setOpenPrice(11.5f);
        quotationInfo.setLowPrice(10.5f);
        quotationInfo.setHighPrice(12.5f);
        quotationInfo.setStockCode(stockCode.toString());
        quotationInfo.setTradeTime(System.currentTimeMillis());
        quotationInfo.setStockName("股票-" + stockCode);

        return quotationInfo;
    }

    public static void main(String[] args) {
        singleThreadProduce();
//        multiThreadProduce();
    }

    private static void singleThreadProduce() {
        ProducerRecord<String, String> record = null;
        StockQuotationInfo quotationInfo = null;
        try {
            int num = 0;
            for (int i = 0; i < MSG_SIZE; i++) {
                quotationInfo = createQuotationInfo();
                record = new ProducerRecord<String, String>(TOPIC, null, quotationInfo.getTradeTime(), quotationInfo.getStockCode(), quotationInfo.toString());
                // 无回调
                Future<RecordMetadata> future  = producer.send(record);
                RecordMetadata metadata = future.get();
                log.info("offset:{}, partition:{}", metadata.offset(), metadata.partition());
                // 有回调
//                producer.send(record, new Callback() {
//                    public void onCompletion(RecordMetadata metadata, Exception exception) {
//                        if (null != exception) {
//                            log.error("Send message occurs exception", exception);
//                        }
//                        if (null != metadata) {
//                            log.info("offset:{}, partition:{}", metadata.offset(), metadata.partition());
//                        }
//                    }
//                });
                if (num++ % 10 == 0) {
                    Thread.sleep(2000L);
                }
            }
        } catch (Exception e) {
            log.error("Send message occurs exception", e);
        } finally {
            producer.close();
        }
    }

    private static void multiThreadProduce() {
        ProducerRecord<String, String> record = null;
        StockQuotationInfo quotationInfo = null;
        int threadNum = 5;
        ExecutorService executor = Executors.newFixedThreadPool(threadNum);
        try {
            for (int i = 0; i < MSG_SIZE; i++) {
                quotationInfo = createQuotationInfo();
                record = new ProducerRecord<String, String>(TOPIC, null, quotationInfo.getTradeTime(), quotationInfo.getStockCode(), quotationInfo.toString());
                executor.submit(new KafkaProducerThread(producer, record));
            }
            System.in.read();
        } catch (Exception e) {
            log.error("Send message occurs exception", e);
        } finally {
            producer.close();
            executor.shutdown();
        }
    }
}
