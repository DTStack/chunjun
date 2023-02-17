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

package com.dtstack.chunjun.sink;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.ContentConfig;
import com.dtstack.chunjun.config.JobConfigBuilder;
import com.dtstack.chunjun.config.OperatorConfig;
import com.dtstack.chunjun.config.SettingConfigBuilder;
import com.dtstack.chunjun.config.SpeedConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.config.SyncConfigBuilder;
import com.dtstack.chunjun.constants.ConfigConstant;
import com.dtstack.chunjun.constants.ConstantValue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.LinkedList;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SinkFactoryTest {

    @Test
    @DisplayName("should use global channel when writer channel is -1")
    public void testInitCommonConfigUseGlobalChannel() {
        SpeedConfig speedConfig = new SpeedConfig();
        speedConfig.setChannel(5);
        speedConfig.setWriterChannel(-1);
        ContentConfig contentConfig = new ContentConfig();
        OperatorConfig writer = new OperatorConfig();
        writer.setParameter(
                ImmutableMap.<String, Object>builder()
                        .put(ConfigConstant.KEY_COLUMN, ImmutableList.of(ConstantValue.STAR_SYMBOL))
                        .build());
        contentConfig.setWriter(writer);
        SyncConfig syncConfig =
                SyncConfigBuilder.newBuilder()
                        .job(
                                JobConfigBuilder.newBuilder()
                                        .setting(
                                                SettingConfigBuilder.newBuilder()
                                                        .speed(speedConfig)
                                                        .build())
                                        .content(new LinkedList<>(ImmutableList.of(contentConfig)))
                                        .build())
                        .build();
        MockSinkFactory sinkFactory = new MockSinkFactory(syncConfig);
        CommonConfig commonConfig = new CommonConfig();
        sinkFactory.initCommonConf(commonConfig);
        assertEquals(5, commonConfig.getParallelism());
    }

    @Test
    @DisplayName("should use writer channel first when writer channel is not -1")
    public void testInitCommonConfigUseWriterChannel() {
        SpeedConfig speedConfig = new SpeedConfig();
        speedConfig.setChannel(5);
        speedConfig.setWriterChannel(3);

        ContentConfig contentConfig = new ContentConfig();
        OperatorConfig writer = new OperatorConfig();
        writer.setParameter(
                ImmutableMap.<String, Object>builder()
                        .put(ConfigConstant.KEY_COLUMN, ImmutableList.of(ConstantValue.STAR_SYMBOL))
                        .build());
        contentConfig.setWriter(writer);
        SyncConfig syncConfig =
                SyncConfigBuilder.newBuilder()
                        .job(
                                JobConfigBuilder.newBuilder()
                                        .setting(
                                                SettingConfigBuilder.newBuilder()
                                                        .speed(speedConfig)
                                                        .build())
                                        .content(new LinkedList<>(ImmutableList.of(contentConfig)))
                                        .build())
                        .build();

        MockSinkFactory sinkFactory = new MockSinkFactory(syncConfig);
        CommonConfig commonConfig = new CommonConfig();
        sinkFactory.initCommonConf(commonConfig);
        assertEquals(3, commonConfig.getParallelism());
    }

    public void testCreateOutput() {
        SpeedConfig speedConfig = new SpeedConfig();
        speedConfig.setChannel(5);
        speedConfig.setWriterChannel(3);

        ContentConfig contentConfig = new ContentConfig();
        OperatorConfig writer = new OperatorConfig();
        writer.setParameter(
                ImmutableMap.<String, Object>builder()
                        .put(ConfigConstant.KEY_COLUMN, ImmutableList.of(ConstantValue.STAR_SYMBOL))
                        .build());
        contentConfig.setWriter(writer);
        SyncConfig syncConfig =
                SyncConfigBuilder.newBuilder()
                        .job(
                                JobConfigBuilder.newBuilder()
                                        .setting(
                                                SettingConfigBuilder.newBuilder()
                                                        .speed(speedConfig)
                                                        .build())
                                        .content(new LinkedList<>(ImmutableList.of(contentConfig)))
                                        .build())
                        .build();

        MockSinkFactory sinkFactory = new MockSinkFactory(syncConfig);
        //        sinkFactory.createOutput()
    }
}
