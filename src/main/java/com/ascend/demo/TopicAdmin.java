package com.ascend.demo;

import kafka.admin.AdminUtils;
import kafka.admin.BrokerMetadata;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.security.JaasUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Map;
import scala.collection.Seq;

import java.util.Properties;

@Slf4j
public class TopicAdmin {
    /**
     * 连接Zk
     */
    private static final String ZK_CONNECT = "10.236.40.159:2181";
    /**
     * session过期时间
     */
    private static final int SESSION_TIMEOUT = 30000;
    /**
     * 连接超时时间
     */
    private static final int CONNECT_TIMEOUT = 30000;

    private static ZkUtils zkUtils = null;

    static {
        zkUtils = ZkUtils.apply(ZK_CONNECT, SESSION_TIMEOUT, CONNECT_TIMEOUT, JaasUtils.isZkSecurityEnabled());
    }

    public static void createTopic(String topic, int partition, int replica, Properties properties) {
        try {
            if (!AdminUtils.topicExists(zkUtils, topic)) {
                AdminUtils.createTopic(zkUtils, topic, partition, replica, properties, AdminUtils.createTopic$default$6());
            } else {
                log.warn("topic [{}] exists", topic);
            }
        } catch (Exception e) {
            handleError(e);
        } finally {
            closeSession();
        }
    }

    public static void modifyTopicConfig(String topic, Properties properties) {
        try {
            Properties curProp = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
            curProp.putAll(properties);

            AdminUtils.changeTopicConfig(zkUtils, topic, curProp);
        } catch (Exception e) {
            handleError(e);
        } finally {
            closeSession();
        }
    }

    public static void addPartitions(String topic, int totalPartitions) {
        try {
            AdminUtils.addPartitions(zkUtils, topic, totalPartitions, null, true, AdminUtils.addPartitions$default$6());
        } catch (Exception e) {
            handleError(e);
        } finally {
            closeSession();
        }
    }

    public static void reassignTopicPartition(String topic, int partition, int replica) {
        try {
            // 获取代理元数据信息，主要是Broker信息
            Seq<BrokerMetadata> brokerMeta = AdminUtils.getBrokerMetadatas(zkUtils,
                    AdminUtils.getBrokerMetadatas$default$2(), AdminUtils.getBrokerMetadatas$default$3());
            // 生成分区副本分配方案
            Map<Object, Seq<Object>> replicaAssign = AdminUtils.assignReplicasToBrokers(brokerMeta, partition, replica,
                    AdminUtils.assignReplicasToBrokers$default$4(), AdminUtils.assignReplicasToBrokers$default$5());
            // 执行方案
            AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, replicaAssign, null, true);
        } catch (Exception e) {
            handleError(e);
        } finally {
            closeSession();
        }
    }

    public static void deleteTopic(String topic) {
        try {
            AdminUtils.deleteTopic(zkUtils, topic);
        } catch (Exception e) {
            handleError(e);
        } finally {
            closeSession();
        }
    }

    private static void closeSession() {
        if (zkUtils != null) {
            zkUtils.close();
        }
    }

    private static void handleError(Exception e) {
        log.error("", e);
    }

    public static void main(String[] args) {
//        createTopic("api-topic-admin", 3, 2, new Properties());

//        Properties properties = new Properties();
//        properties.put("max.message.bytes", "404800");
//        modifyTopicConfig("api-topic-admin", properties);

//        addPartitions("api-topic-admin", 5);

//        reassignTopicPartition("api-topic-admin", 2, 3);

        deleteTopic("api-topic-admin");
    }
}
