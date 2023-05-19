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
import com.dtstack.chunjun.connector.stream.source.StreamInputFormatBuilder;
import com.dtstack.chunjun.connector.stream.util.StreamConfigUtil;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;

public class StreamDynamicTableSource implements ScanTableSource {

    private final ResolvedSchema schema;
    private final StreamConfig streamConfig;
    private final DataType physicalRowDataType;

    public StreamDynamicTableSource(
            ResolvedSchema schema, StreamConfig streamConfig, DataType physicalRowDataType) {
        this.schema = schema;
        this.streamConfig = streamConfig;
        this.physicalRowDataType = physicalRowDataType;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final TypeInformation<RowData> typeInformation =
                InternalTypeInfo.of(physicalRowDataType.getLogicalType());

        StreamConfigUtil.extractFieldConfig(schema, streamConfig);

        StreamInputFormatBuilder builder = new StreamInputFormatBuilder();
        builder.setRowConverter(
                new StreamSqlConverter(
                        InternalTypeInfo.of(physicalRowDataType.getLogicalType()).toRowType()));
        builder.setStreamConf(streamConfig);

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                streamConfig.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new StreamDynamicTableSource(schema, streamConfig, physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "StreamDynamicTableSource:";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }
}
