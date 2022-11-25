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

package com.dtstack.chunjun.connector.emqx.sink;

import com.dtstack.chunjun.connector.emqx.conf.EmqxConf;
import com.dtstack.chunjun.connector.emqx.converter.EmqxRowConverter;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import static com.dtstack.chunjun.connector.emqx.util.DataTypeConventerUtil.createValueFormatProjection;

public class EmqxDynamicTableSink implements DynamicTableSink {

    private final TableSchema physicalSchema;
    private final EmqxConf emqxConf;
    /** Format for encoding values from emqx. */
    private final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat;

    public EmqxDynamicTableSink(
            TableSchema physicalSchema,
            EmqxConf emqxConf,
            EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat) {
        this.physicalSchema = physicalSchema;
        this.emqxConf = emqxConf;
        this.valueEncodingFormat =
                Preconditions.checkNotNull(
                        valueEncodingFormat, "Value encoding format must not be null.");
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return valueEncodingFormat.getChangelogMode();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context runtimeProviderContext) {
        EmqxOutputFormatBuilder builder = new EmqxOutputFormatBuilder();
        builder.setEmqxConf(emqxConf);
        builder.setRowConverter(
                new EmqxRowConverter(
                        valueEncodingFormat.createRuntimeEncoder(
                                runtimeProviderContext,
                                DataTypeUtils.projectRow(
                                        physicalSchema.toRowDataType(),
                                        createValueFormatProjection(
                                                physicalSchema.toRowDataType())))));

        return SinkFunctionProvider.of(new DtOutputFormatSinkFunction<>(builder.finish()), 1);
    }

    @Override
    public DynamicTableSink copy() {
        return new EmqxDynamicTableSink(physicalSchema, emqxConf, valueEncodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "EMQX sink";
    }
}
