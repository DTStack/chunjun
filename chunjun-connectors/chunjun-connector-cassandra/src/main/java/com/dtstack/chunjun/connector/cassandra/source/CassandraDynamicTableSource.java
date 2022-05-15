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

package com.dtstack.chunjun.connector.cassandra.source;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.cassandra.conf.CassandraLookupConf;
import com.dtstack.chunjun.connector.cassandra.conf.CassandraSourceConf;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraRawTypeConverter;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraRowConverter;
import com.dtstack.chunjun.connector.cassandra.lookup.CassandraAllTableFunction;
import com.dtstack.chunjun.connector.cassandra.lookup.CassandraLruTableFunction;
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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraDynamicTableSource implements ScanTableSource, LookupTableSource {

    private static final String IDENTIFIER = "Cassandra";

    private final CassandraSourceConf sourceConf;

    private final CassandraLookupConf cassandraLookupConf;

    private final TableSchema tableSchema;

    public CassandraDynamicTableSource(
            CassandraSourceConf sourceConf,
            CassandraLookupConf cassandraLookupConf,
            TableSchema tableSchema) {
        this.sourceConf = sourceConf;
        this.cassandraLookupConf = cassandraLookupConf;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        CassandraInputFormatBuilder builder = new CassandraInputFormatBuilder();

        LogicalType logicalType = tableSchema.toRowDataType().getLogicalType();
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(logicalType);

        String[] fieldNames = tableSchema.getFieldNames();
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
        List<String> columnNameList = new ArrayList<>();

        for (int i = 0; i < fieldNames.length; i++) {
            String name = fieldNames[i];
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
            field.setIndex(i);

            columnList.add(field);
        }
        sourceConf.setColumn(columnList);

        RowType rowType =
                TableUtil.createRowType(sourceConf.getColumn(), CassandraRawTypeConverter::apply);

        builder.setSourceConf(sourceConf);
        builder.setRowConverter(new CassandraRowConverter(rowType, columnNameList));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInfo),
                false,
                sourceConf.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new CassandraDynamicTableSource(sourceConf, cassandraLookupConf, tableSchema);
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

        if (cassandraLookupConf.getCache().equalsIgnoreCase(CacheType.LRU.toString())) {
            return ParallelAsyncTableFunctionProvider.of(
                    new CassandraLruTableFunction(
                            cassandraLookupConf,
                            new CassandraRowConverter(
                                    rowType, Arrays.asList(tableSchema.getFieldNames())),
                            tableSchema.getFieldNames(),
                            keyNames),
                    cassandraLookupConf.getParallelism());
        }
        return ParallelTableFunctionProvider.of(
                new CassandraAllTableFunction(
                        cassandraLookupConf,
                        new CassandraRowConverter(
                                rowType, Arrays.asList(tableSchema.getFieldNames())),
                        tableSchema.getFieldNames(),
                        keyNames),
                cassandraLookupConf.getParallelism());
    }
}
