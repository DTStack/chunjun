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

package com.dtstack.flinkx.connector.pgwal.source;

import com.dtstack.flinkx.connector.pgwal.conf.PGWalConf;
import com.dtstack.flinkx.connector.pgwal.converter.PGWalRowConverter;
import com.dtstack.flinkx.connector.pgwal.inputformat.PGWalInputFormatBuilder;
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

import com.google.common.base.Preconditions;

/** */
public class PGWalDynamicTableSource implements ScanTableSource {
    private final TableSchema schema;
    private final PGWalConf conf;
    private final TimestampFormat timestampFormat;

    public PGWalDynamicTableSource(
            TableSchema schema, PGWalConf conf, TimestampFormat timestampFormat) {
        this.schema = schema;
        this.conf = conf;
        this.timestampFormat = timestampFormat;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        Preconditions.checkArgument(
                schema.toRowDataType().getLogicalType() instanceof RowType,
                "schema cannot cast to RowType");
        final RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

        PGWalInputFormatBuilder builder = new PGWalInputFormatBuilder();
        builder.setConf(conf);
        builder.setRowConverter(
                new PGWalRowConverter(
                        (RowType) this.schema.toRowDataType().getLogicalType(),
                        this.timestampFormat));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation), false, 1);
    }

    @Override
    public DynamicTableSource copy() {
        return new PGWalDynamicTableSource(this.schema, this.conf, this.timestampFormat);
    }

    @Override
    public String asSummaryString() {
        return "PGWalDynamicTableSource:";
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
