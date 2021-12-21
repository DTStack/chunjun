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

package com.dtstack.flinkx.connector.binlog.source;

import com.dtstack.flinkx.connector.binlog.conf.BinlogConf;
import com.dtstack.flinkx.connector.binlog.converter.BinlogRowConverter;
import com.dtstack.flinkx.connector.binlog.inputformat.BinlogInputFormatBuilder;
import com.dtstack.flinkx.source.DtInputFormatSourceFunction;
import com.dtstack.flinkx.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * @author chuixue
 * @create 2021-04-09 09:20
 * @description
 */
public class BinlogDynamicTableSource implements ScanTableSource {
    private final TableSchema schema;
    private final BinlogConf binlogConf;
    private final TimestampFormat timestampFormat;

    public BinlogDynamicTableSource(
            TableSchema schema, BinlogConf binlogConf, TimestampFormat timestampFormat) {
        this.schema = schema;
        this.binlogConf = binlogConf;
        this.timestampFormat = timestampFormat;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

        BinlogInputFormatBuilder builder = new BinlogInputFormatBuilder();
        builder.setBinlogConf(binlogConf);
        builder.setRowConverter(
                new BinlogRowConverter(
                        (RowType) this.schema.toRowDataType().getLogicalType(),
                        this.timestampFormat));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation), false, 1);
    }

    @Override
    public DynamicTableSource copy() {
        return new BinlogDynamicTableSource(this.schema, this.binlogConf, this.timestampFormat);
    }

    @Override
    public String asSummaryString() {
        return "BinlogDynamicTableSource:";
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
}
