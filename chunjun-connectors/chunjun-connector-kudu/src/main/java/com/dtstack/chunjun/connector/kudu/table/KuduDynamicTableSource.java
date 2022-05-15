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

package com.dtstack.chunjun.connector.kudu.table;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.kudu.conf.KuduLookupConf;
import com.dtstack.chunjun.connector.kudu.conf.KuduSourceConf;
import com.dtstack.chunjun.connector.kudu.converter.KuduRawTypeConverter;
import com.dtstack.chunjun.connector.kudu.converter.KuduRowConverter;
import com.dtstack.chunjun.connector.kudu.source.KuduInputFormatBuilder;
import com.dtstack.chunjun.connector.kudu.table.lookup.KuduAllTableFunction;
import com.dtstack.chunjun.connector.kudu.table.lookup.KuduLruTableFunction;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncTableFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelTableFunctionProvider;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author tiezhu
 * @since 2021/6/9 星期三
 */
public class KuduDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    private static final String IDENTIFIER = "Kudu";

    private final KuduSourceConf sourceConf;

    private final KuduLookupConf kuduLookupConf;

    private TableSchema tableSchema;

    public KuduDynamicTableSource(
            KuduSourceConf sourceConf, KuduLookupConf lookupConf, TableSchema tableSchema) {
        this.sourceConf = sourceConf;
        this.tableSchema = tableSchema;
        this.kuduLookupConf = lookupConf;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        KuduInputFormatBuilder builder = new KuduInputFormatBuilder();

        LogicalType logicalType = tableSchema.toRowDataType().getLogicalType();
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(logicalType);

        String[] fieldNames = tableSchema.getFieldNames();
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
        List<String> columnNameList = new ArrayList<>();

        for (int index = 0; index < fieldNames.length; index++) {
            String name = fieldNames[index];
            columnNameList.add(name);

            FieldConf field = new FieldConf();
            field.setName(name);
            field.setType(
                    tableSchema
                            .getFieldDataType(name)
                            .orElse(new AtomicDataType(new NullType()))
                            .getLogicalType()
                            .getTypeRoot()
                            .name());
            field.setIndex(index);
            columnList.add(field);
        }

        sourceConf.setColumn(columnList);

        RowType rowType =
                TableUtil.createRowType(sourceConf.getColumn(), KuduRawTypeConverter::apply);

        builder.setKuduSourceConf(sourceConf);
        builder.setRowConverter(new KuduRowConverter(rowType, columnNameList));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInfo),
                true,
                sourceConf.getParallelism());
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public DynamicTableSource copy() {
        return new KuduDynamicTableSource(sourceConf, kuduLookupConf, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return IDENTIFIER;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "Kudu only support non-nested look up keys");
            keyNames[i] = tableSchema.getFieldNames()[innerKeyArr[0]];
        }
        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

        if (kuduLookupConf.getCache().equalsIgnoreCase(CacheType.LRU.toString())) {
            return ParallelAsyncTableFunctionProvider.of(
                    new KuduLruTableFunction(
                            kuduLookupConf,
                            new KuduRowConverter(
                                    rowType, Arrays.asList(tableSchema.getFieldNames())),
                            tableSchema.getFieldNames(),
                            keyNames),
                    kuduLookupConf.getParallelism());
        }
        return ParallelTableFunctionProvider.of(
                new KuduAllTableFunction(
                        kuduLookupConf,
                        new KuduRowConverter(rowType, Arrays.asList(tableSchema.getFieldNames())),
                        tableSchema.getFieldNames(),
                        keyNames),
                kuduLookupConf.getParallelism());
    }

    @Override
    public boolean supportsNestedProjection() {
        // kudu doesn't support nested projection
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {
        this.tableSchema = TableSchemaUtils.projectSchema(tableSchema, projectedFields);
    }
}
