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

package com.dtstack.flinkx.connector.emqx.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.Preconditions;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.emqx.conf.EmqxConf;
import com.dtstack.flinkx.connector.emqx.converter.EmqxRowConverter;
import com.dtstack.flinkx.streaming.api.functions.source.DtInputFormatSourceFunction;
import com.dtstack.flinkx.table.connector.source.ParallelSourceFunctionProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * @author chuixue
 * @create 2021-06-01 20:11
 * @description
 */
public class EmqxDynamicTableSource implements ScanTableSource {

    private final TableSchema physicalSchema;
    private final EmqxConf emqxConf;
    /** Format for decoding values from emqx. */
    private final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat;

    public EmqxDynamicTableSource(
            TableSchema physicalSchema,
            EmqxConf emqxConf,
            DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat) {
        this.physicalSchema = physicalSchema;
        this.emqxConf = emqxConf;
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
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(rowType);

        EmqxInputFormatBuilder builder = new EmqxInputFormatBuilder();
        String[] fieldNames = physicalSchema.getFieldNames();
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
        int index = 0;
        for (String name : fieldNames) {
            FieldConf field = new FieldConf();
            field.setName(name);
            field.setIndex(index++);
            columnList.add(field);
        }
        emqxConf.setColumn(columnList);

        builder.setEmqxConf(emqxConf);
        builder.setRowConverter(
                new EmqxRowConverter(
                        valueDecodingFormat.createRuntimeDecoder(
                                runtimeProviderContext,
                                DataTypeUtils.projectRow(
                                        physicalSchema.toRowDataType(),
                                        createValueFormatProjection(
                                                physicalSchema.toRowDataType())))));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                emqxConf.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new EmqxDynamicTableSource(physicalSchema, emqxConf, valueDecodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "emqx";
    }

    private static int[] createValueFormatProjection(DataType physicalDataType) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(
                hasRoot(physicalType, LogicalTypeRoot.ROW), "Row data type expected.");
        final int physicalFieldCount = LogicalTypeChecks.getFieldCount(physicalType);
        final IntStream physicalFields = IntStream.range(0, physicalFieldCount);

        return physicalFields.toArray();
    }
}
