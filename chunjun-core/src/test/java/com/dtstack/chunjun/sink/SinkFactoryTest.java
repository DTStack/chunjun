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

import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.conf.ContentConf;
import com.dtstack.chunjun.conf.JobConfBuilder;
import com.dtstack.chunjun.conf.OperatorConf;
import com.dtstack.chunjun.conf.SettingConfBuilder;
import com.dtstack.chunjun.conf.SpeedConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.conf.SyncConfBuilder;
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
    public void testInitChunJunCommonConfUseGlobalChannel() {
        SpeedConf speedConf = new SpeedConf();
        speedConf.setChannel(5);
        speedConf.setWriterChannel(-1);
        ContentConf contentConf = new ContentConf();
        OperatorConf writer = new OperatorConf();
        writer.setParameter(
                ImmutableMap.<String, Object>builder()
                        .put(ConfigConstant.KEY_COLUMN, ImmutableList.of(ConstantValue.STAR_SYMBOL))
                        .build());
        contentConf.setWriter(writer);
        SyncConf syncConf =
                SyncConfBuilder.newBuilder()
                        .job(
                                JobConfBuilder.newBuilder()
                                        .setting(
                                                SettingConfBuilder.newBuilder()
                                                        .speed(speedConf)
                                                        .build())
                                        .content(new LinkedList<>(ImmutableList.of(contentConf)))
                                        .build())
                        .build();
        MockSinkFactory sinkFactory = new MockSinkFactory(syncConf);
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        sinkFactory.initCommonConf(chunJunCommonConf);
        assertEquals(5, chunJunCommonConf.getParallelism());
    }

    @Test
    @DisplayName("should use writer channel first when writer channel is not -1")
    public void testInitChunJunCommonConfUseWriterChannel() {
        SpeedConf speedConf = new SpeedConf();
        speedConf.setChannel(5);
        speedConf.setWriterChannel(3);

        ContentConf contentConf = new ContentConf();
        OperatorConf writer = new OperatorConf();
        writer.setParameter(
                ImmutableMap.<String, Object>builder()
                        .put(ConfigConstant.KEY_COLUMN, ImmutableList.of(ConstantValue.STAR_SYMBOL))
                        .build());
        contentConf.setWriter(writer);
        SyncConf syncConf =
                SyncConfBuilder.newBuilder()
                        .job(
                                JobConfBuilder.newBuilder()
                                        .setting(
                                                SettingConfBuilder.newBuilder()
                                                        .speed(speedConf)
                                                        .build())
                                        .content(new LinkedList<>(ImmutableList.of(contentConf)))
                                        .build())
                        .build();

        MockSinkFactory sinkFactory = new MockSinkFactory(syncConf);
        ChunJunCommonConf chunJunCommonConf = new ChunJunCommonConf();
        sinkFactory.initCommonConf(chunJunCommonConf);
        assertEquals(3, chunJunCommonConf.getParallelism());
    }

    public void testCreateOutput() {
        SpeedConf speedConf = new SpeedConf();
        speedConf.setChannel(5);
        speedConf.setWriterChannel(3);

        ContentConf contentConf = new ContentConf();
        OperatorConf writer = new OperatorConf();
        writer.setParameter(
                ImmutableMap.<String, Object>builder()
                        .put(ConfigConstant.KEY_COLUMN, ImmutableList.of(ConstantValue.STAR_SYMBOL))
                        .build());
        contentConf.setWriter(writer);
        SyncConf syncConf =
                SyncConfBuilder.newBuilder()
                        .job(
                                JobConfBuilder.newBuilder()
                                        .setting(
                                                SettingConfBuilder.newBuilder()
                                                        .speed(speedConf)
                                                        .build())
                                        .content(new LinkedList<>(ImmutableList.of(contentConf)))
                                        .build())
                        .build();

        MockSinkFactory sinkFactory = new MockSinkFactory(syncConf);
        //        sinkFactory.createOutput()
    }
}
