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

package com.dtstack.chunjun.source;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.config.ContentConfig;
import com.dtstack.chunjun.config.JobConfig;
import com.dtstack.chunjun.config.OperatorConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.constants.ConfigConstant;
import com.dtstack.chunjun.constants.ConstantValue;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SourceFactoryTest {

    @BeforeEach
    public void setup() {}

    @Test
    public void testStarWithTransformer() {
        SyncConfig syncConfig = new SyncConfig();
        JobConfig jobConfig = new JobConfig();
        ContentConfig contentConfig = new ContentConfig();
        OperatorConfig reader = new OperatorConfig();
        reader.setParameter(
                ImmutableMap.<String, Object>builder()
                        .put(ConfigConstant.KEY_COLUMN, ImmutableList.of(ConstantValue.STAR_SYMBOL))
                        .build());
        contentConfig.setReader(reader);
        jobConfig.setContent(new LinkedList<>(ImmutableList.of(contentConfig)));
        syncConfig.setJob(jobConfig);
        MockSourceFactory sourceFactory =
                new MockSourceFactory(syncConfig, new DummyStreamExecutionEnvironment());
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setColumn(reader.getFieldList());
        IllegalArgumentException thrownA =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> sourceFactory.checkConstant(commonConfig),
                        "Expected checkConstant() to throw, but it didn't");
        assertEquals("in transformer mode : not support '*' in column.", thrownA.getMessage());
    }

    @Test
    public void testDefaultValueWithTransformer() {
        SyncConfig syncConfig = new SyncConfig();
        JobConfig jobConfig = new JobConfig();
        ContentConfig contentConfig = new ContentConfig();
        OperatorConfig reader = new OperatorConfig();
        reader.setParameter(
                ImmutableMap.<String, Object>builder()
                        .put(
                                ConfigConstant.KEY_COLUMN,
                                ImmutableList.of(
                                        ImmutableMap.builder()
                                                .put("name", "id")
                                                .put("type", "int")
                                                .put("index", 0)
                                                .put("value", 123)
                                                .build()))
                        .build());
        contentConfig.setReader(reader);
        jobConfig.setContent(new LinkedList<>(ImmutableList.of(contentConfig)));
        syncConfig.setJob(jobConfig);
        MockSourceFactory sourceFactory =
                new MockSourceFactory(syncConfig, new DummyStreamExecutionEnvironment());
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setColumn(reader.getFieldList());
        IllegalArgumentException thrownA =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> sourceFactory.checkConstant(commonConfig),
                        "Expected checkConstant() to throw, but it didn't");
        assertEquals(
                "in transformer mode : not support default value,you can set value in transformer",
                thrownA.getMessage());
    }

    @Test
    public void testFormatWithTransformer() {
        SyncConfig syncConfig = new SyncConfig();
        JobConfig jobConfig = new JobConfig();
        ContentConfig contentConfig = new ContentConfig();
        OperatorConfig reader = new OperatorConfig();
        reader.setParameter(
                ImmutableMap.<String, Object>builder()
                        .put(
                                ConfigConstant.KEY_COLUMN,
                                ImmutableList.of(
                                        ImmutableMap.builder()
                                                .put("name", "id")
                                                .put("type", "int")
                                                .put("index", 0)
                                                .put("format", "yyyy-MM-dd hh:mm:ss")
                                                .build()))
                        .build());
        contentConfig.setReader(reader);
        jobConfig.setContent(new LinkedList<>(ImmutableList.of(contentConfig)));
        syncConfig.setJob(jobConfig);
        MockSourceFactory sourceFactory =
                new MockSourceFactory(syncConfig, new DummyStreamExecutionEnvironment());
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setColumn(reader.getFieldList());
        IllegalArgumentException thrownA =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> sourceFactory.checkConstant(commonConfig),
                        "Expected checkConstant() to throw, but it didn't");
        assertEquals(
                "in transformer mode : not support default format,you can set format in transformer",
                thrownA.getMessage());
    }

    @Test
    public void testCreateInput() {
        SyncConfig syncConfig = new SyncConfig();
        JobConfig jobConfig = new JobConfig();
        ContentConfig contentConfig = new ContentConfig();
        OperatorConfig reader = new OperatorConfig();
        reader.setParameter(
                ImmutableMap.<String, Object>builder()
                        .put(
                                ConfigConstant.KEY_COLUMN,
                                ImmutableList.of(
                                        ImmutableMap.builder()
                                                .put("name", "id")
                                                .put("type", "int")
                                                .put("index", 0)
                                                .put("format", "yyyy-MM-dd hh:mm:ss")
                                                .build()))
                        .build());
        contentConfig.setReader(reader);
        jobConfig.setContent(new LinkedList<>(ImmutableList.of(contentConfig)));
        syncConfig.setJob(jobConfig);
        /*MockSourceFactory sourceFactory =
        new MockSourceFactory(syncConfig, new DummyStreamExecutionEnvironment());
        sourceFactory.createInput()*/
    }

    public static class DummyStreamExecutionEnvironment extends StreamExecutionEnvironment {

        private final Map<String, String> cachedFileMap = new HashMap<>();

        @Override
        public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
            return null;
        }

        @Override
        public void registerCachedFile(String filePath, String name, boolean executable) {
            cachedFileMap.put(name, filePath);
        }

        public Map<String, String> getCachedFileMap() {
            return cachedFileMap;
        }
    }
}
