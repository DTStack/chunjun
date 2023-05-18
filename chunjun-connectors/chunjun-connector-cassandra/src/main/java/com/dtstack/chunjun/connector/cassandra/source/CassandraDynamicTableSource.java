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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.cassandra.config.CassandraLookupConfig;
import com.dtstack.chunjun.connector.cassandra.config.CassandraSourceConfig;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraRawTypeConverter;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraSqlConverter;
import com.dtstack.chunjun.connector.cassandra.lookup.CassandraAllTableFunction;
import com.dtstack.chunjun.connector.cassandra.lookup.CassandraLruTableFunction;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class CassandraDynamicTableSource implements ScanTableSource, LookupTableSource {

    private static final String IDENTIFIER = "Cassandra";

    private final CassandraSourceConfig sourceConfig;

    private final CassandraLookupConfig cassandraLookupConfig;

    private final ResolvedSchema tableSchema;

    private final DataType dataType;

    public CassandraDynamicTableSource(
            CassandraSourceConfig sourceConfig,
            CassandraLookupConfig cassandraLookupConfig,
            ResolvedSchema tableSchema,
            DataType dataType) {
        this.sourceConfig = sourceConfig;
        this.cassandraLookupConfig = cassandraLookupConfig;
        this.tableSchema = tableSchema;
        this.dataType = dataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        CassandraInputFormatBuilder builder = new CassandraInputFormatBuilder();

        LogicalType logicalType = dataType.getLogicalType();
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(logicalType);

        List<Column> columns = tableSchema.getColumns();
        List<FieldConfig> columnList = new ArrayList<>(columns.size());
        List<String> columnNameList = new ArrayList<>(columns.size());

        for (int index = 0; index < columns.size(); index++) {
            Column column = columns.get(index);
            String name = column.getName();
            columnNameList.add(name);

            FieldConfig field = new FieldConfig();

            field.setName(name);
            field.setType(
                    TypeConfig.fromString(column.getDataType().getLogicalType().asSummaryString()));
            field.setIndex(index);

            columnList.add(field);
        }
        sourceConfig.setColumn(columnList);

        RowType rowType =
                TableUtil.createRowType(sourceConfig.getColumn(), CassandraRawTypeConverter::apply);

        builder.setSourceConf(sourceConfig);
        builder.setRowConverter(new CassandraSqlConverter(rowType, columnNameList));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInfo),
                false,
                sourceConfig.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new CassandraDynamicTableSource(
                sourceConfig, cassandraLookupConfig, tableSchema, dataType);
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
            keyNames[i] = tableSchema.getColumnNames().get(innerKeyArr[0]);
        }
        if (cassandraLookupConfig.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelLookupFunctionProvider.of(
                    new CassandraAllTableFunction(
                            cassandraLookupConfig,
                            new CassandraSqlConverter(
                                    InternalTypeInfo.of(dataType.getLogicalType()).toRowType(),
                                    tableSchema.getColumnNames()),
                            tableSchema.getColumnNames().toArray(new String[0]),
                            keyNames),
                    cassandraLookupConfig.getParallelism());
        }
        return ParallelAsyncLookupFunctionProvider.of(
                new CassandraLruTableFunction(
                        cassandraLookupConfig,
                        new CassandraSqlConverter(
                                InternalTypeInfo.of(dataType.getLogicalType()).toRowType(),
                                tableSchema.getColumnNames()),
                        tableSchema.getColumnNames().toArray(new String[0]),
                        keyNames),
                cassandraLookupConfig.getParallelism());
    }
}
