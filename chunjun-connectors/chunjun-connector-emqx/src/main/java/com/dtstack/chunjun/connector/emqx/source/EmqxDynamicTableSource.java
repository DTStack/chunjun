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

package com.dtstack.chunjun.connector.emqx.source;

import com.dtstack.chunjun.connector.emqx.config.EmqxConfig;
import com.dtstack.chunjun.connector.emqx.converter.EmqxSqlConverter;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

public class EmqxDynamicTableSource implements ScanTableSource {

    private final ResolvedSchema physicalSchema;
    private final EmqxConfig emqxConfig;
    /** Format for decoding values from emqx. */
    private final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    public EmqxDynamicTableSource(
            ResolvedSchema physicalSchema,
            EmqxConfig emqxConfig,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat) {
        this.physicalSchema = physicalSchema;
        this.emqxConfig = emqxConfig;
        this.valueDecodingFormat =
                Preconditions.checkNotNull(
                        valueDecodingFormat, "Value decoding format must not be null.");
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        LogicalType logicalType = physicalSchema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(logicalType);
        EmqxInputFormatBuilder builder = new EmqxInputFormatBuilder();
        builder.setEmqxConf(emqxConfig);
        builder.setRowConverter(
                new EmqxSqlConverter(
                        valueDecodingFormat.createRuntimeDecoder(
                                runtimeProviderContext, physicalSchema.toPhysicalRowDataType())));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                emqxConfig.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new EmqxDynamicTableSource(physicalSchema, emqxConfig, valueDecodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "emqx";
    }
}
