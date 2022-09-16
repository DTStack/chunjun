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

package com.dtstack.chunjun.connector.kafka.deserializer;

import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class DtKafkaDeserializerTest {

    @Test
    public void testKafkaDeserializer() {
        DtKafkaDeserializer dtKafkaDeserializer = new DtKafkaDeserializer();
        HashMap<Object, Object> configure = new HashMap<>();
        configure.put(
                "dt.key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configure.put(
                "dt.value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        dtKafkaDeserializer.configure(configure, true);
        dtKafkaDeserializer.configure(configure, false);

        String data = "{\"id\":\"1\"}";
        byte[] tests =
                dtKafkaDeserializer.deserialize("test", data.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(data, new String(tests));

        tests =
                dtKafkaDeserializer.deserialize(
                        "test", null, data.getBytes(StandardCharsets.UTF_8));
        Assert.assertEquals(data, new String(tests));

        dtKafkaDeserializer.close();
    }

    @Test(expected = ChunJunRuntimeException.class)
    public void testKafkaDeserializerWithError() {
        DtKafkaDeserializer dtKafkaDeserializer = new DtKafkaDeserializer();
        HashMap<Object, Object> configure = new HashMap<>();
        configure.put("dt.key.deserializer", "org.apache.kafka.common.serialization.aaaa");
        configure.put(
                "dt.value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        dtKafkaDeserializer.configure(configure, true);
    }
}
