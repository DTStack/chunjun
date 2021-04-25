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
        PowerMockito.when(KafkaUtil.getTopicListFromBroker(consumerSettings)).thenCallRealMethod();
    }

    @Test
    public void getTopicPartitionCountAndReplicasTest(){
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        String topic = "kafka10";
        PowerMockito.when(KafkaUtil.getTopicPartitionCountAndReplicas(consumerSettings, topic)).thenCallRealMethod();
    }

    @Test
    public void listConsumerGroupTest(){
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        String topic = "kafka10";
        PowerMockito.when(KafkaUtil.listConsumerGroup(consumerSettings, topic)).thenCallRealMethod();
    }

    @Test
    public void getGroupInfoByGroupIdTest(){
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        String topic = "kafka10";
        PowerMockito.when(KafkaUtil.getGroupInfoByGroupId(consumerSettings, "", topic)).thenCallRealMethod();
    }

    @Test
    public void initProperties(){
        Map<String, String> consumerSettings = new HashMap<>();
        consumerSettings.put("bootstrap.servers", "flinkx1:9092");
        PowerMockito.when(KafkaUtil.initProperties(consumerSettings)).thenCallRealMethod();
    }
}
