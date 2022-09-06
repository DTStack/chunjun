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

package com.dtstack.chunjun.connector.oceanbasecdc.source;

import com.dtstack.chunjun.connector.oceanbasecdc.conf.OceanBaseCdcConf;
import com.dtstack.chunjun.connector.oceanbasecdc.converter.OceanBaseCdcRowConverter;
import com.dtstack.chunjun.connector.oceanbasecdc.inputformat.OceanBaseCdcInputFormatBuilder;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

public class OceanBaseCdcDynamicTableSource implements ScanTableSource {

    private final TableSchema schema;
    private final OceanBaseCdcConf cdcConf;
    private final TimestampFormat timestampFormat;

    public OceanBaseCdcDynamicTableSource(
            TableSchema schema, OceanBaseCdcConf cdcConf, TimestampFormat timestampFormat) {
        this.schema = schema;
        this.cdcConf = cdcConf;
        this.timestampFormat = timestampFormat;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .addContainedKind(RowKind.DELETE)
                .build();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

        OceanBaseCdcInputFormatBuilder builder = new OceanBaseCdcInputFormatBuilder();
        builder.setOceanBaseCdcConf(cdcConf);
        builder.setRowConverter(
                new OceanBaseCdcRowConverter(
                        (RowType) this.schema.toRowDataType().getLogicalType(),
                        this.timestampFormat));
        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation), false, 1);
    }

    @Override
    public DynamicTableSource copy() {
        return new OceanBaseCdcDynamicTableSource(schema, cdcConf, timestampFormat);
    }

    @Override
    public String asSummaryString() {
        return "OceanBaseCdcDynamicTableSource";
    }
}
