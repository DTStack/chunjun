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

package com.dtstack.chunjun.connector.kafka.source;

import com.dtstack.chunjun.Main;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.environment.MyLocalStreamEnvironment;
import com.dtstack.chunjun.options.Options;

import org.apache.flink.configuration.Configuration;

import com.sun.tools.javac.util.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/** @author dujie 2022/08/09 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({KafkaSourceFactory.class})
public class KafkaSourceFactoryTest {

    private MyLocalStreamEnvironment env;
    private String job =
            "{\n"
                    + "  \"job\": {\n"
                    + "    \"content\": [\n"
                    + "      {\n"
                    + "        \"reader\": {\n"
                    + "          \"parameter\": {\n"
                    + "            \"topic\": \"da\",\n"
                    + "            %s,\n"
                    + "            \"groupId\": \"dddd\",\n"
                    + "            \"codec\": \"json\",\n"
                    + "            \"consumerSettings\": {\n"
                    + "              \"bootstrap.servers\": \"localhost:9092\",\n"
                    + "              \"auto.commit.enable\": \"false\"\n"
                    + "            }\n"
                    + "          },\n"
                    + "          \"name\": \"kafkasource\"\n"
                    + "        },\n"
                    + "        \"writer\": {\n"
                    + "          \"name\": \"streamwriter\",\n"
                    + "          \"parameter\": {\n"
                    + "            \"print\": true\n"
                    + "          }\n"
                    + "        }\n"
                    + "      }\n"
                    + "    ],\n"
                    + "    \"setting\": {\n"
                    + "      \"restore\": {\n"
                    + "        \"isRestore\": true,\n"
                    + "        \"isStream\": true\n"
                    + "      },\n"
                    + "      \"speed\": {\n"
                    + "        \"readerChannel\": 1,\n"
                    + "        \"writerChannel\": 1\n"
                    + "      }\n"
                    + "    }\n"
                    + "  }\n"
                    + "}\n";
    private KafkaSourceFactory factory;

    @Before
    public void setup() {
        Configuration conf = new Configuration();
        conf.setString("akka.ask.timeout", "180 s");
        conf.setString("web.timeout", String.valueOf(100000));
        env = new MyLocalStreamEnvironment(conf);
    }

    @Test
    public void KafkaSourceFactoryTest() {
        SyncConf config =
                Main.parseConf(
                        String.format(job, "  \"mode\": \"earliest-offset\""), new Options());
        factory = new KafkaSourceFactory(config, env);
        Assert.checkNonNull(factory.createSource());

        config = Main.parseConf(String.format(job, "  \"mode\": \"latest-offset\""), new Options());
        factory = new KafkaSourceFactory(config, env);
        Assert.checkNonNull(factory.createSource());

        config =
                Main.parseConf(
                        String.format(
                                job,
                                " \"mode\": \"timestamp\",\"timestamp\":"
                                        + System.currentTimeMillis()),
                        new Options());
        factory = new KafkaSourceFactory(config, env);
        Assert.checkNonNull(factory.createSource());

        config =
                Main.parseConf(
                        String.format(
                                job,
                                " \"mode\": \"specific-offsets\",\"offset\":\"partition:0,offset:42;partition:1,offset:300\""),
                        new Options());
        factory = new KafkaSourceFactory(config, env);
        Assert.checkNonNull(factory.createSource());
    }
}
