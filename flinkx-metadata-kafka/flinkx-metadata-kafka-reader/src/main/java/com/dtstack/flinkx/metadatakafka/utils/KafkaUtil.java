package com.dtstack.flinkx.metadatakafka.utils;


import com.dtstack.flinkx.metadatakafka.entity.KafkaConsumerInfo;
import com.dtstack.flinkx.metadatakafka.inputformat.MetadatakafkaInputFormat;
import com.google.common.collect.Lists;
import kafka.coordinator.group.GroupOverview;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;


public class KafkaUtil {

    private static Logger LOG = LoggerFactory.getLogger(MetadatakafkaInputFormat.class);

    private static final String KEY_PARTITIONS = "partitions";

    private static final String KEY_REPLICAS = "replicas";


    /**
     * 从kafka中获取topic信息
     *
     * @return topic list
     */
    public static List<String> getTopicListFromBroker(Map<String, String> consumerSettings) throws Exception {
        Properties props = initProperties(consumerSettings);
        List<String> results = Lists.newArrayList();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            if (topics != null) {
                results.addAll(topics.keySet());
            }
        } catch (Exception e) {
           throw new Exception(e);
        }
        return results;
    }

    /**
     * 获取topic的分区数和副本数
     *
     * @return 分区数和副本数
     */
    public static Map<String, Integer> getTopicPartitionCountAndReplicas(Map<String, String> consumerSettings, String topic) throws Exception {
        Properties props = initProperties(consumerSettings);
        Properties clientProp = removeExtraParam(props);
        AdminClient client = AdminClient.create(clientProp);
        //存放结果
        Map<String, Integer> countAndReplicas = new HashMap<>();

        DescribeTopicsResult result = client.describeTopics(Arrays.asList(topic));
        Map<String, KafkaFuture<TopicDescription>> values = result.values();
        KafkaFuture<TopicDescription> topicDescription = values.get(topic);
        int partitions = 0,replicas = 0;
        try {
            partitions = topicDescription.get().partitions().size();
            replicas = topicDescription.get().partitions().iterator().next().replicas().size();
        } catch (Exception e) {
            LOG.error("get topic partition count and replicas error:{}", e.getMessage(), e);
            throw new Exception(e);
        } finally {
            client.close();
        }
        countAndReplicas.put(KEY_PARTITIONS, partitions);
        countAndReplicas.put(KEY_REPLICAS, replicas);
        return  countAndReplicas;
    }


    /**
     * 获取 kafka 消费者组列表
     *
     */
    public static List<String> listConsumerGroup(Map<String, String> consumerSettings, String topic) throws Exception {
        List<String> consumerGroups =  Lists.newArrayList();
        Properties props = initProperties(consumerSettings);
        Properties clientProp = removeExtraParam(props);
        // 获取kafka client
        kafka.admin.AdminClient adminClient = kafka.admin.AdminClient.create(clientProp);
        try {
            // scala seq 转 java list
            List<GroupOverview> groups = JavaConversions.seqAsJavaList(adminClient.listAllConsumerGroupsFlattened().toSeq());
            groups.forEach(group -> consumerGroups.add(group.groupId()));
            // 不指定topic 全部返回
            if (StringUtils.isBlank(topic)) {
                return consumerGroups;
            }
            List<String> consumerGroupsByTopic = Lists.newArrayList();
            for (String groupId : consumerGroups) {
                kafka.admin.AdminClient.ConsumerGroupSummary groupSummary = adminClient.describeConsumerGroup(groupId, 5000L);
                // 消费者组不存在的情况
                if (Objects.isNull(groupSummary) || "Dead".equals(groupSummary.state())) {
                    continue;
                }
                Map<TopicPartition, Object> offsets = JavaConversions.mapAsJavaMap(adminClient.listGroupOffsets(groupId));
                for (TopicPartition topicPartition : offsets.keySet()) {
                    if (topic.equals(topicPartition.topic())) {
                        consumerGroupsByTopic.add(groupId);
                        break;
                    }
                }
            }
            return consumerGroupsByTopic;
        } catch (Exception e){
            LOG.error("get consumer group list error:{}", e.getMessage(), e);
            throw new Exception(e);
        } finally {
            if (Objects.nonNull(adminClient)) {
                adminClient.close();
            }
        }
    }

    /**
     * 获取 kafka 消费者组详细信息
     */
    public static List<KafkaConsumerInfo> getGroupInfoByGroupId(Map<String, String> consumerSettings, String groupId, String srcTopic) throws Exception {
        List<KafkaConsumerInfo> result = Lists.newArrayList();
        Properties props = initProperties(consumerSettings);
        Properties clientProp = removeExtraParam(props);
        // 获取kafka client
        kafka.admin.AdminClient adminClient = kafka.admin.AdminClient.create(clientProp);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)){

            if (StringUtils.isNotBlank(groupId)) {
                kafka.admin.AdminClient.ConsumerGroupSummary groupSummary = adminClient.describeConsumerGroup(groupId, 5000L);
                // 消费者组不存在的情况
                if (Objects.isNull(groupSummary) || "Dead".equals(groupSummary.state())) {
                    return result;
                }
            }else {
                // groupId 为空的时候获取所有的分区
                List<PartitionInfo> allPartitions = consumer.partitionsFor(srcTopic);
                for (PartitionInfo partitionInfo : allPartitions) {
                    TopicPartition topicPartition = new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
                    // 指定当前分区
                    consumer.assign(Lists.newArrayList(topicPartition));
                    consumer.seekToEnd(Lists.newArrayList(topicPartition));
                    long logEndOffset = consumer.position(topicPartition);
                    String brokerHost = Objects.isNull(partitionInfo.leader()) ? null : partitionInfo.leader().host();
                    // 组装kafka consumer 信息
                    KafkaConsumerInfo kafkaConsumerInfo = new KafkaConsumerInfo();

                    kafkaConsumerInfo.setGroupId(groupId);
                    kafkaConsumerInfo.setTopic(partitionInfo.topic());
                    kafkaConsumerInfo.setPartition(partitionInfo.partition());
                    kafkaConsumerInfo.setLogEndOffset(logEndOffset);
                    kafkaConsumerInfo.setBrokerHost(brokerHost);
                    result.add(kafkaConsumerInfo);
                }
                return result;
            }

            Map<TopicPartition, Object> offsets = JavaConversions.mapAsJavaMap(adminClient.listGroupOffsets(groupId));
            for (TopicPartition topicPartition : offsets.keySet()) {
                String topic = topicPartition.topic();
                // 过滤指定topic 下的 partition
                if (StringUtils.isNotBlank(srcTopic) && !srcTopic.equals(topic)) {
                    continue;
                }
                int partition = topicPartition.partition();
                // 当前消费位置
                Long currentOffset = (Long) offsets.get(topicPartition);
                List<TopicPartition> singleTopicPartition = Lists.newArrayList(topicPartition);
                // 指定当前分区
                consumer.assign(singleTopicPartition);
                consumer.seekToEnd(singleTopicPartition);
                long logEndOffset = consumer.position(topicPartition);

                List<PartitionInfo> partitions = consumer.partitionsFor(topic);

                // 组装kafka consumer 信息
                KafkaConsumerInfo kafkaConsumerInfo = new KafkaConsumerInfo();

                kafkaConsumerInfo.setGroupId(groupId);
                kafkaConsumerInfo.setTopic(topic);
                kafkaConsumerInfo.setPartition(partition);
                kafkaConsumerInfo.setCurrentOffset(currentOffset);
                kafkaConsumerInfo.setLogEndOffset(logEndOffset);
                kafkaConsumerInfo.setLag(logEndOffset - currentOffset);

                // 查询当前分区 leader 所在机器的host
                for (PartitionInfo partitionInfo : partitions) {
                    if (partition == partitionInfo.partition() && Objects.nonNull(partitionInfo.leader())) {
                        kafkaConsumerInfo.setBrokerHost(partitionInfo.leader().host());
                        break;
                    }
                }
                result.add(kafkaConsumerInfo);
            }
        } catch (Exception e) {
            LOG.error("query groupInfo by groupId error:{}", e.getMessage(), e);
            throw new Exception(e);
        } finally {
            if (Objects.nonNull(adminClient)) {
                adminClient.close();
            }
        }
        return result;
    }


    /**
     * 初始化kafka配置参数
     */
    public synchronized static Properties initProperties(Map<String, String> consumerSettings) {
        LOG.info("Initialize Kafka configuration information, consumerSettings : {}", consumerSettings);
        Properties props = new Properties();
        props.putAll(consumerSettings);
        /* 是否自动确认offset */
        props.put("enable.auto.commit", "true");
        /* 自动确认offset的时间间隔 */
        props.put("auto.commit.interval.ms", "1000");
        //heart beat 默认3s
        props.put("session.timeout.ms", "10000");
        //一次性的最大拉取条数
        props.put("max.poll.records", "5");
        props.put("auto.offset.reset", "earliest");
        /* key的序列化类 */
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        /* value的序列化类 */
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /*设置超时时间*/
        props.put("request.timeout.ms", "10500");
        return props;
    }

    /**
     * 删除properties中kafka client 不需要的的参数
     * @param properties properties
     * @return prop
     */
    private static Properties removeExtraParam(Properties properties){
        Properties prop = new Properties();
        prop.putAll(properties);
        //以下这些参数kafka client不需要
        prop.remove("enable.auto.commit");
        prop.remove("auto.commit.interval.ms");
        prop.remove("session.timeout.ms");
        prop.remove("max.poll.records");
        prop.remove("auto.offset.reset");
        prop.remove("key.deserializer");
        prop.remove("value.deserializer");
        return prop;
    }

}
