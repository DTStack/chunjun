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

package com.dtstack.chunjun.connector.jdbc.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcRawTypeConverterTest;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.converter.RawTypeMapper;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
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

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest({JdbcUtil.class, Connection.class})
public class SinkFactoryTest {

    private static String json;

    @BeforeClass
    public static void setup() throws IOException {
        mockStatic(JdbcUtil.class);

        DataStream<RowData> dataStream = mock(DataStream.class);
        DataStreamSink<RowData> dataStreamSink = mock(DataStreamSink.class);
        when(dataStream.addSink(any())).thenReturn(dataStreamSink);
        json = readFile("sync_test.json");
    }

    @Test
    public void initTest() {
        SyncConfig syncConfig = SyncConfig.parseJob(json);
        TestSinkFactory sinkFactory =
                new TestSinkFactory(
                        syncConfig,
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

        List<String> name = new ArrayList<>();
        List<TypeConfig> type = new ArrayList<>();
        syncConfig
                .getReader()
                .getFieldList()
                .forEach(
                        field -> {
                            name.add(field.getName());
                            type.add(field.getType());
                        });
        Pair<List<String>, List<TypeConfig>> pair = mock(Pair.class);
        when(JdbcUtil.buildColumnWithMeta(any(), any(), any())).thenAnswer(invocation -> pair);

        when(pair.getLeft()).thenReturn(name);
        when(pair.getRight()).thenReturn(type);
    }

    public static class TestSinkFactory extends JdbcSinkFactory {
        public TestSinkFactory(SyncConfig syncConfig, JdbcDialect jdbcDialect) {
            super(syncConfig, jdbcDialect);
        }
    }
}
