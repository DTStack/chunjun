package com.dtstack.flinkx.metadatakafka.utils;


import com.dtstack.flinkx.metadatakafka.entity.KafkaConsumerInfo;
import com.dtstack.flinkx.metadatakafka.inputformat.MetadatakafkaInputFormat;
import com.google.common.collect.Lists;
import kafka.coordinator.group.GroupOverview;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
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
import sun.security.krb5.Config;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;


public class KafkaUtil {

    private static Logger LOG = LoggerFactory.getLogger(MetadatakafkaInputFormat.class);

    private static final String KEY_JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    private static final String KAFKA_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";

    private static final String KAFKA_KERBEROS_KEYTAB = "kafka.kerberos.keytab";

    private static final String PRINCIPAL_FILE = "principalFile";

    private static final String PRINCIPAL = "principal";

    private static final String KEY_PARTITIONS = "partitions";

    private static final String KEY_REPLICAS = "replicas";

    public static String KAFKA_JAAS_CONTENT = "KafkaClient {\n" +
            "    com.sun.security.auth.module.Krb5LoginModule required\n" +
            "    useKeyTab=true\n" +
            "    storeKey=true\n" +
            "    keyTab=\"%s\"\n" +
            "    principal=\"%s\";\n" +
            "};";

    /**
     * 从kafka中获取topic信息
     *
     * @return topic list
     */
    public static List<String> getTopicListFromBroker(Map<String, String> consumerSettings, Map<String, Object> kerberosConfig) {
        Properties props = initProperties(consumerSettings, kerberosConfig);
        List<String> results = Lists.newArrayList();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            Map<String, List<PartitionInfo>> topics = consumer.listTopics();
            if (topics != null) {
                results.addAll(topics.keySet());
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }finally {
            destroyProperty();
        }
        return results;
    }

    /**
     * 获取topic的分区数和副本数
     *
     * @return 分区数和副本数
     */
    public static Map<String, Integer> getTopicPartitionCountAndReplicas(Map<String, String> consumerSettings, Map<String, Object> kerberosConfig, String topic){
        Properties props = initProperties(consumerSettings, kerberosConfig);
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
            LOG.error("getTopicPartitionCountAndReplicas error:{}", e.getMessage(), e);
        } finally {
            client.close();
            destroyProperty();
        }
        countAndReplicas.put(KEY_PARTITIONS, partitions);
        countAndReplicas.put(KEY_REPLICAS, replicas);
        return  countAndReplicas;
    }


    /**
     * 获取 kafka 消费者组列表
     *
     */
    public static List<String> listConsumerGroup(Map<String, String> consumerSettings, Map<String, Object> kerberosConfig, String topic) {
        List<String> consumerGroups = new ArrayList<>();
        Properties props = initProperties(consumerSettings, kerberosConfig);
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
            LOG.error("listConsumerGroup error:{}", e.getMessage(), e);
        } finally {
            if (Objects.nonNull(adminClient)) {
                adminClient.close();
            }
            destroyProperty();
        }
        return Lists.newArrayList();
    }

    /**
     * 获取 kafka 消费者组详细信息
     */
    public static List<KafkaConsumerInfo> getGroupInfoByGroupId(Map<String, String> consumerSettings, Map<String, Object> kerberosConfig, String groupId, String srcTopic) {
        List<KafkaConsumerInfo> result = Lists.newArrayList();
        Properties props = initProperties(consumerSettings, kerberosConfig);
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
            LOG.error("getGroupInfoByGroupId error:{}", e.getMessage(), e);
        } finally {
            if (Objects.nonNull(adminClient)) {
                adminClient.close();
            }
            destroyProperty();
        }
        return result;
    }


    /**
     * 初始化kafka配置参数
     */
    public synchronized static Properties initProperties(Map<String, String> consumerSettings, Map<String, Object> kerberosConfig) {
        LOG.info("Initialize Kafka configuration information, consumerSettings : {}, kerberosConfig : {}", consumerSettings, kerberosConfig);
        Properties props = new Properties();
        for (Map.Entry<String, String> entry : consumerSettings.entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();
            props.put(k, v);
        }
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
        if (MapUtils.isEmpty(kerberosConfig)) {
            return props;
        }
        // 只需要认证的用户名
        String kafkaKbrServiceName = MapUtils.getString(kerberosConfig, KAFKA_KERBEROS_SERVICE_NAME);
        kafkaKbrServiceName = kafkaKbrServiceName.split("/")[0];
        String kafkaLoginConf = writeKafkaJaas(kerberosConfig);

        // 刷新kerberos认证信息，在设置完java.security.krb5.conf后进行，否则会使用上次的krb5文件进行 refresh 导致认证失败
        try {
            Config.refresh();
            javax.security.auth.login.Configuration.setConfiguration(null);
        } catch (Exception e) {
            LOG.error("Kafka kerberos authentication information refresh failed!");
        }
        // kerberos 相关设置
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        // kafka broker的启动配置
        props.put("sasl.kerberos.service.name", kafkaKbrServiceName);
        System.setProperty("java.security.auth.login.config", kafkaLoginConf);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        return props;
    }

    public static void destroyProperty() {
        System.clearProperty("java.security.auth.login.config");
        System.clearProperty("javax.security.auth.useSubjectCredsOnly");
    }

    /**
     * 写kafka jaas文件，同时处理 krb5.conf
     * @param kerberosConfig kerberos配置
     * @return jaas文件绝对路径
     */
    private static String writeKafkaJaas(Map<String, Object> kerberosConfig) {
        LOG.info("Initialize Kafka JAAS file, kerberosConfig : {}", kerberosConfig);
        if (MapUtils.isEmpty(kerberosConfig)) {
            return null;
        }

        // 处理 krb5.conf
        if (kerberosConfig.containsKey(KEY_JAVA_SECURITY_KRB5_CONF)) {
            System.setProperty(KEY_JAVA_SECURITY_KRB5_CONF, MapUtils.getString(kerberosConfig, KEY_JAVA_SECURITY_KRB5_CONF));
        }

        String keytabConf = MapUtils.getString(kerberosConfig, PRINCIPAL_FILE);
        // 兼容历史数据
        keytabConf = StringUtils.isBlank(keytabConf) ? MapUtils.getString(kerberosConfig, KAFKA_KERBEROS_KEYTAB) : keytabConf;
        try {
            File file = new File(keytabConf);
            File jaas = new File(file.getParent() + File.separator + "kafka_jaas.conf");
            if (jaas.exists()) {
                jaas.delete();
            }
            String principal = MapUtils.getString(kerberosConfig, PRINCIPAL);
            // 历史数据兼容
            principal = StringUtils.isBlank(principal) ? MapUtils.getString(kerberosConfig, "kafka.kerberos.principal") : principal;
            FileUtils.write(jaas, String.format(KAFKA_JAAS_CONTENT, keytabConf, principal));
            String kafkaLoginConf = jaas.getAbsolutePath();
            LOG.info("Init Kafka Kerberos:login-conf:{}\n --sasl.kerberos.service.name:{}", keytabConf, principal);
            return kafkaLoginConf;
        } catch (IOException e) {
            throw new RuntimeException(String.format("Writing to Kafka configuration file exception,%s", e.getMessage()), e);
        }
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
