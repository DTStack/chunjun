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

package com.dtstack.chunjun.connector.kafka.adapter;

import com.dtstack.chunjun.connector.kafka.conf.KafkaConf;
import com.dtstack.chunjun.connector.kafka.enums.StartupMode;
import com.dtstack.chunjun.util.GsonUtil;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Assert;
import org.junit.Test;

public class StartupModeAdapterTest {

    @Test
    public void testStartupModeAdapter() {
        Gson gson =
                new GsonBuilder()
                        .registerTypeAdapter(StartupMode.class, new StartupModeAdapter())
                        .create();
        GsonUtil.setTypeAdapter(gson);
        KafkaConf kafkaConf = new KafkaConf();
        kafkaConf.setMode(StartupMode.LATEST);

        String s = gson.toJson(kafkaConf);

        Assert.assertTrue(s.contains("latest-offset"));
    }
}
