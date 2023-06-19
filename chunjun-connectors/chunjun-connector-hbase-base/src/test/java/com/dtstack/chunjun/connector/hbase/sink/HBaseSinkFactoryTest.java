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

package com.dtstack.chunjun.connector.hbase.sink;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.util.HBaseTestUtil;
import com.dtstack.chunjun.converter.RawTypeMapper;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HBaseSinkFactoryTest {

    private DataStream<RowData> dataStream;

    private DataStreamSink<RowData> dataStreamSink;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws IOException {
        dataStream = (DataStream<RowData>) mock(DataStream.class);
        dataStreamSink = (DataStreamSink<RowData>) mock(DataStreamSink.class);
    }

    @Test
    public void testGetRawTypeConverter() throws IOException {
        String job = HBaseTestUtil.readFile("stream_hbase_2.json");
        SyncConfig conf = SyncConfig.parseJob(job);
        HBaseSinkFactoryBase sinkFactory = new TestSinkFactory(conf);
        RawTypeMapper converter = sinkFactory.getRawTypeMapper();

        HBaseTableSchema hbaseTest =
                sinkFactory.buildHBaseTableSchema("hbase_test", conf.getWriter().getFieldList());

        Assert.assertEquals("stu", hbaseTest.getFamilyNames()[0]);
        Assert.assertEquals(DataTypes.NULL(), converter.apply(TypeConfig.fromString("NULL")));
    }

    @Test
    public void testCreateSink() throws IOException {
        String job = HBaseTestUtil.readFile("stream_hbase_2.json");
        HBaseSinkFactoryBase sinkFactory = new TestSinkFactory(SyncConfig.parseJob(job));
        when(dataStream.addSink(any())).thenReturn(dataStreamSink);
        sinkFactory.createSink(dataStream);
    }

    public static final class TestSinkFactory extends HBaseSinkFactoryBase {
        public TestSinkFactory(SyncConfig config) {
            super(config);
        }
    }
}
