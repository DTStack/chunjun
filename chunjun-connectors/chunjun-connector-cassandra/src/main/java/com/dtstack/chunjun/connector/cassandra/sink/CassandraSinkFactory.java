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

package com.dtstack.chunjun.connector.cassandra.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.cassandra.conf.CassandraSinkConf;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraColumnConverter;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraRawTypeConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraSinkFactory extends SinkFactory {

    private final CassandraSinkConf sinkConf;

    public CassandraSinkFactory(SyncConf syncConf) {
        super(syncConf);
        sinkConf =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()),
                        CassandraSinkConf.class);
        sinkConf.setColumn(syncConf.getWriter().getFieldList());
        super.initCommonConf(sinkConf);
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return CassandraRawTypeConverter::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        CassandraOutputFormatBuilder builder = new CassandraOutputFormatBuilder();

        builder.setSinkConf(sinkConf);
        List<FieldConf> fieldConfList = sinkConf.getColumn();

        final RowType rowType = TableUtil.createRowType(fieldConfList, getRawTypeConverter());
        builder.setRowConverter(new CassandraColumnConverter(rowType, fieldConfList));

        return createOutput(dataSet, builder.finish());
    }
}
