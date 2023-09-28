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

package com.dtstack.chunjun.connector.jdbc.source.distribute;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcRawTypeConverterTest;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.converter.RawTypeMapper;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static com.dtstack.chunjun.connector.jdbc.util.JdbcUtilTest.readFile;
import static org.powermock.api.mockito.PowerMockito.mock;

public class SourceFactoryTest {

    private static TestSourceFactory sourceFactory;
    private static StreamExecutionEnvironment env;
    private static String json;

    @Before
    public void setup() throws IOException {
        env = mock(StreamExecutionEnvironment.class);
        json = readFile("distribute_sync_test.json");
    }

    @Test
    public void initTest() {
        sourceFactory =
                new TestSourceFactory(
                        SyncConfig.parseJob(json),
                        env,
                        new JdbcDialect() {
                            @Override
                            public String dialectName() {
                                return "test";
                            }

                            @Override
                            public boolean canHandle(String url) {
                                return true;
                            }

                            @Override
                            public RawTypeMapper getRawTypeConverter() {
                                return JdbcRawTypeConverterTest::apply;
                            }
                        });
    }

    public static class TestSourceFactory extends DistributedJdbcSourceFactory {
        public TestSourceFactory(
                SyncConfig syncConfig, StreamExecutionEnvironment env, JdbcDialect jdbcDialect) {
            super(syncConfig, env, jdbcDialect);
        }
    }
}
