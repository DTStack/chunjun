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

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcRawTypeConverterTest;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.converter.RawTypeConverter;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static com.dtstack.chunjun.connector.jdbc.util.JdbcUtilTest.readFile;
import static org.mockito.ArgumentMatchers.any;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/** @author liuliu 2022/8/19 */
public class SinkFactoryTest {

    private static TestSinkFactory sinkFactory;
    private static DataStream<RowData> dataStream;
    private static DataStreamSink<RowData> dataStreamSink;
    private static String json;

    @BeforeClass
    public static void setup() throws IOException {
        dataStream = mock(DataStream.class);
        dataStreamSink = mock(DataStreamSink.class);
        when(dataStream.addSink(any())).thenReturn(dataStreamSink);
        json = readFile("sync_test.json");
    }

    @Test
    public void initTest() {
        sinkFactory =
                new TestSinkFactory(
                        SyncConf.parseJob(json),
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
        sinkFactory.createSink(dataStream);
    }

    public static class TestSinkFactory extends JdbcSinkFactory {
        public TestSinkFactory(SyncConf syncConf, JdbcDialect jdbcDialect) {
            super(syncConf, jdbcDialect);
        }
    }
}
