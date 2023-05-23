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

package com.dtstack.chunjun.connector.stream.table;

import com.dtstack.chunjun.connector.stream.config.StreamConfig;
import com.dtstack.chunjun.connector.stream.converter.StreamSqlConverter;
import com.dtstack.chunjun.connector.stream.sink.StreamOutputFormatBuilder;
import com.dtstack.chunjun.connector.stream.util.StreamConfigUtil;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;

public class StreamDynamicTableSink implements DynamicTableSink {

    private final StreamConfig streamConfig;
    private final LogicalType logicalType;
    private final ResolvedSchema schema;

    public StreamDynamicTableSink(
            StreamConfig streamConfig, LogicalType logicalType, ResolvedSchema schema) {
        this.streamConfig = streamConfig;
        this.logicalType = logicalType;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        final InternalTypeInfo<?> typeInformation = InternalTypeInfo.of(logicalType);

        StreamConfigUtil.extractFieldConfig(schema, streamConfig);

        StreamOutputFormatBuilder builder = new StreamOutputFormatBuilder();
        builder.setStreamConfig(streamConfig);
        builder.setRowConverter(new StreamSqlConverter(typeInformation.toRowType()));

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), streamConfig.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new StreamDynamicTableSink(streamConfig, logicalType, schema);
    }

    @Override
    public String asSummaryString() {
        return "StreamDynamicTableSink";
    }
}
