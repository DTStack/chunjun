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

package com.dtstack.chunjun.connector.ftp.source;

import com.dtstack.chunjun.connector.ftp.conf.FtpConfig;
import com.dtstack.chunjun.connector.ftp.converter.FtpRowConverter;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/06/19
 */
public class FtpDynamicTableSource implements ScanTableSource {

    private TableSchema schema;
    private FtpConfig ftpConfig;
    private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public FtpDynamicTableSource(
            TableSchema schema,
            FtpConfig ftpConfig,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        this.schema = schema;
        this.ftpConfig = ftpConfig;
        this.decodingFormat = decodingFormat;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final RowType rowType = (RowType) schema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

        FtpInputFormatBuilder builder = new FtpInputFormatBuilder();
        builder.setFtpConfig(ftpConfig);
        builder.setRowConverter(
                new FtpRowConverter(
                        decodingFormat.createRuntimeDecoder(
                                runtimeProviderContext, schema.toRowDataType())));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation), false, 1);
    }

    @Override
    public DynamicTableSource copy() {
        return new FtpDynamicTableSource(schema, ftpConfig, decodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "FtpDynamicTableSource: ";
    }
}
