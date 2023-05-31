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

package com.dtstack.chunjun.connector.file.table;

import com.dtstack.chunjun.config.BaseFileConfig;
import com.dtstack.chunjun.connector.file.converter.FileSqlConverter;
import com.dtstack.chunjun.connector.file.source.FileInputFormatBuilder;
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
import org.apache.flink.table.types.DataType;

public class FileDynamicTableSource implements ScanTableSource {

    private final ResolvedSchema schema;
    private final BaseFileConfig fileConfig;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public FileDynamicTableSource(
            ResolvedSchema schema,
            BaseFileConfig fileConfig,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        this.schema = schema;
        this.fileConfig = fileConfig;
        this.decodingFormat = decodingFormat;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        DataType dataType = schema.toPhysicalRowDataType();

        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(dataType.getLogicalType());

        FileInputFormatBuilder builder = new FileInputFormatBuilder();
        builder.setFileConf(fileConfig);
        builder.setRowConverter(
                new FileSqlConverter(
                        decodingFormat.createRuntimeDecoder(runtimeProviderContext, dataType)));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                true,
                fileConfig.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new FileDynamicTableSource(schema, fileConfig, decodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "file";
    }
}
