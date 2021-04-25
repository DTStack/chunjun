package com.dtstack.flinkx.metadatakafka.utils;

import com.dtstack.flinkx.metadatakafka.inputformat.MetadatakafkaInputFormat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;

/**
 * Company：www.dtstack.com
 *
 * @author shitou
 * @date 2021/4/25 15:36
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ KafkaUtil.class, MetadatakafkaInputFormat.class})
public class KafkaUtilTest {


    @Before
    public void before(){
        PowerMockito.mockStatic(KafkaUtil.class);
    }

    @Test
    public void getTopicListFromBrokerTest(){
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        Map<String, Object> kerberosConfig = new HashMap<>();
        PowerMockito.when(KafkaUtil.getTopicListFromBroker(consumerSettings, kerberosConfig)).thenCallRealMethod();
    }

    @Test
    public void getTopicPartitionCountAndReplicasTest(){
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        Map<String, Object> kerberosConfig = new HashMap<>();
        String topic = "kafka10";
        PowerMockito.when(KafkaUtil.getTopicPartitionCountAndReplicas(consumerSettings, kerberosConfig, topic)).thenCallRealMethod();
    }

    @Test
    public void listConsumerGroupTest(){
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        Map<String, Object> kerberosConfig = new HashMap<>();
        String topic = "kafka10";
        PowerMockito.when(KafkaUtil.listConsumerGroup(consumerSettings, kerberosConfig, topic)).thenCallRealMethod();
    }

    @Test
    public void getGroupInfoByGroupIdTest(){
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        Map<String, Object> kerberosConfig = new HashMap<>();
        String groupId = "default";
        String topic = "kafka10";
        PowerMockito.when(KafkaUtil.getGroupInfoByGroupId(consumerSettings, kerberosConfig, groupId, topic)).thenCallRealMethod();
    }

    @Test
    public void initProperties(){
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        Map<String, Object> kerberosConfig = new HashMap<>();
        kerberosConfig.put("sasl.kerberos.service.name", "kafka");
        kerberosConfig.put("java.security.krb5.conf", "D:/Temp");
        kerberosConfig.put("kafka.kerberos.keytab", "D:/Temp");
        kerberosConfig.put("kafka.kerberos.principal", "kafka/flinkx@DTSTACK.COM");
        PowerMockito.when(KafkaUtil.initProperties(consumerSettings, kerberosConfig)).thenCallRealMethod();
    }
}
