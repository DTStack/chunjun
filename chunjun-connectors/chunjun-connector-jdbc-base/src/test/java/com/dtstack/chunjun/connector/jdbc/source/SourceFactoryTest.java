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

package com.dtstack.chunjun.connector.jdbc.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcRawTypeConverterTest;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.converter.RawTypeConverter;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static com.dtstack.chunjun.connector.jdbc.util.JdbcUtilTest.readFile;
import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

/** @author liuliu 2022/8/15 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcUtil.class, Connection.class})
public class SourceFactoryTest {

    private static TestSourceFactory sourceFactory;
    private static StreamExecutionEnvironment env;
    private static String json;

    private static SyncConf syncConf;

    @Before
    public void setup() throws IOException {
        mockStatic(JdbcUtil.class);

        env = mock(StreamExecutionEnvironment.class);
        json = readFile("sync_test.json");
        syncConf = SyncConf.parseJob(json);
        sourceFactory =
                new TestSourceFactory(
                        syncConf,
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
                            public RawTypeConverter getRawTypeConverter() {
                                return JdbcRawTypeConverterTest::apply;
                            }
                        });
    }

    @Test
    public void initTest() {

        List<String> name = new ArrayList<>();
        List<String> type = new ArrayList<>();
        syncConf.getReader()
                .getFieldList()
                .forEach(
                        field -> {
                            name.add(field.getName());
                            type.add(field.getType());
                        });
        Pair<List<String>, List<String>> pair = mock(Pair.class);
        when(JdbcUtil.getTableMetaData(any(), any(), any(), any())).thenAnswer(invocation -> pair);
        when(JdbcUtil.buildColumnWithMeta(any(), any(), any())).thenAnswer(invocation -> pair);

        when(pair.getLeft()).thenReturn(name);
        when(pair.getRight()).thenReturn(type);
        sourceFactory.createSource();
    }

    @Test
    public void getDefaultFetchSizeTest() {
        assert sourceFactory.getDefaultFetchSize() == 1024;
    }

    public static class TestSourceFactory extends JdbcSourceFactory {
        public TestSourceFactory(
                SyncConf syncConf, StreamExecutionEnvironment env, JdbcDialect jdbcDialect) {
            super(syncConf, env, jdbcDialect);
        }
    }
}
