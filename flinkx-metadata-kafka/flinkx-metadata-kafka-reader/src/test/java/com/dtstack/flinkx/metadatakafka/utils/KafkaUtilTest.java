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
import java.util.Properties;

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
    public void getTopicListFromBrokerTest() throws Exception{
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        Properties properties = KafkaUtil.initProperties(consumerSettings);
        PowerMockito.when(KafkaUtil.getTopicListFromBroker(properties)).thenCallRealMethod();
    }

    @Test
    public void getTopicPartitionCountAndReplicasTest() throws Exception{
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        Properties properties = KafkaUtil.initProperties(consumerSettings);
        String topic = "kafka10";
        PowerMockito.when(KafkaUtil.getTopicPartitionCountAndReplicas(properties, topic)).thenCallRealMethod();
    }

    @Test
    public void listConsumerGroupTest() throws Exception{
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        Properties properties = KafkaUtil.initProperties(consumerSettings);
        String topic = "kafka10";
        PowerMockito.when(KafkaUtil.listConsumerGroup(properties, topic)).thenCallRealMethod();
    }

    @Test
    public void getGroupInfoByGroupIdTest() throws Exception{
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        Properties properties = KafkaUtil.initProperties(consumerSettings);
        String topic = "kafka10";
        PowerMockito.when(KafkaUtil.getGroupInfoByGroupId(properties, "", topic)).thenCallRealMethod();
    }

    @Test
    public void initProperties(){
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        PowerMockito.when(KafkaUtil.initProperties(consumerSettings)).thenCallRealMethod();
    }
}
