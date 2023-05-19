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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.cassandra.config.CassandraSinkConfig;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraRawTypeConverter;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraSyncConverter;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

public class CassandraSinkFactory extends SinkFactory {

    private final CassandraSinkConfig sinkConfig;

    public CassandraSinkFactory(SyncConfig syncConfig) {
        super(syncConfig);
        sinkConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConfig.getWriter().getParameter()),
                        CassandraSinkConfig.class);
        sinkConfig.setColumn(syncConfig.getWriter().getFieldList());
        super.initCommonConf(sinkConfig);
    }

    @Override
    public RawTypeMapper getRawTypeMapper() {
        return CassandraRawTypeConverter::apply;
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        CassandraOutputFormatBuilder builder = new CassandraOutputFormatBuilder();

        builder.setSinkConfig(sinkConfig);
        List<FieldConfig> fieldList = sinkConfig.getColumn();

        final RowType rowType = TableUtil.createRowType(fieldList, getRawTypeMapper());
        builder.setRowConverter(
                new CassandraSyncConverter(rowType, fieldList), useAbstractBaseColumn);

        return createOutput(dataSet, builder.finish());
    }
}
