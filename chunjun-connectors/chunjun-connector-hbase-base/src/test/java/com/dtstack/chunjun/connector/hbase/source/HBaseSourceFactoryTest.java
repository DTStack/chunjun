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

package com.dtstack.chunjun.connector.hbase.source;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.hbase.util.HBaseTestUtil;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.throwable.UnsupportedTypeException;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class HBaseSourceFactoryTest {

    private StreamExecutionEnvironment env;

    private DataStreamSource<RowData> stream;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        env = mock(StreamExecutionEnvironment.class);
        stream = (DataStreamSource<RowData>) mock((DataStreamSource.class));
    }

    @Test
    public void testSourceFactoryForSyncWithTransform() throws IOException {
        SyncConfig conf =
                SyncConfig.parseJob(HBaseTestUtil.readFile("hbase_stream_with_transform.json"));
        HBaseSourceFactoryBase factory = new TestSourceFactory(conf, env);

        RawTypeConverter converter = factory.getRawTypeConverter();
        Assert.assertThrows(UnsupportedTypeException.class, () -> converter.apply("Test"));

        when(env.addSource(any(), anyString(), any()))
                .thenAnswer((Answer<DataStreamSource<RowData>>) answer -> stream);

        factory.createSource();
    }

    @Test
    public void testSourceFactoryForSync() throws IOException {
        SyncConfig conf = SyncConfig.parseJob(HBaseTestUtil.readFile("hbase_stream.json"));
        HBaseSourceFactoryBase factory = new TestSourceFactory(conf, env);

        RawTypeConverter converter = factory.getRawTypeConverter();
        Assert.assertThrows(UnsupportedTypeException.class, () -> converter.apply("Test"));

        when(env.addSource(any(), anyString(), any()))
                .thenAnswer((Answer<DataStreamSource<RowData>>) answer -> stream);

        factory.createSource();
    }

    public static final class TestSourceFactory extends HBaseSourceFactoryBase {

        public TestSourceFactory(SyncConfig syncConfig, StreamExecutionEnvironment env) {
            super(syncConfig, env);
        }
    }
}
