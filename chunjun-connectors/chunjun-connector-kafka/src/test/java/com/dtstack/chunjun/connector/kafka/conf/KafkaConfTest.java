/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.chunjun.connector.kafka.conf;

import com.dtstack.chunjun.connector.kafka.enums.StartupMode;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class KafkaConfTest {

    @Test
    public void testConfiguration() {
        KafkaConf kafkaConf = new KafkaConf();
        Assert.assertEquals(StartupMode.GROUP_OFFSETS, kafkaConf.getMode());
        Assert.assertEquals("text", kafkaConf.getCodec());

        kafkaConf.setMode(StartupMode.LATEST);
        Assert.assertEquals(StartupMode.LATEST, kafkaConf.getMode());

        kafkaConf.setCodec("json");
        Assert.assertEquals("json", kafkaConf.getCodec());

        kafkaConf.setTopic("test");
        Assert.assertEquals("test", kafkaConf.getTopic());

        kafkaConf.setGroupId("test");
        Assert.assertEquals("test", kafkaConf.getGroupId());

        kafkaConf.setOffset("1000");
        Assert.assertEquals("1000", kafkaConf.getOffset());

        Assert.assertEquals(-1, kafkaConf.getTimestamp());
        kafkaConf.setTimestamp(1983342301L);
        Assert.assertEquals(1983342301L, kafkaConf.getTimestamp());

        HashMap<String, String> configs = new HashMap<>();
        configs.put("bootstrap.servers", "localhost:9092");
        kafkaConf.setConsumerSettings(configs);
        Assert.assertEquals(configs, kafkaConf.getConsumerSettings());

        kafkaConf.setProducerSettings(configs);
        Assert.assertEquals(configs, kafkaConf.getProducerSettings());

        kafkaConf.setSplit(true);
        Assert.assertTrue(kafkaConf.isSplit());

        kafkaConf.setPavingData(true);
        Assert.assertTrue(kafkaConf.isPavingData());

        kafkaConf.setDataCompelOrder(true);
        Assert.assertTrue(kafkaConf.isDataCompelOrder());

        kafkaConf.setDeserialization("string");
        Assert.assertEquals("string", kafkaConf.getDeserialization());
    }
}
