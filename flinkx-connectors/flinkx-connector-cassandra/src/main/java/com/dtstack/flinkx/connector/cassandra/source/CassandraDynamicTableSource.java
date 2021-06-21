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

package com.dtstack.flinkx.connector.cassandra.source;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.cassandra.conf.CassandraSourceConf;
import com.dtstack.flinkx.connector.cassandra.converter.CassandraColumnConverter;
import com.dtstack.flinkx.connector.cassandra.converter.CassandraRawTypeConverter;
import com.dtstack.flinkx.streaming.api.functions.source.DtInputFormatSourceFunction;
import com.dtstack.flinkx.table.connector.source.ParallelSourceFunctionProvider;
import com.dtstack.flinkx.util.TableUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraDynamicTableSource implements ScanTableSource {

    private static final String IDENTIFIER = "Cassandra";

    private final CassandraSourceConf sourceConf;

    private final TableSchema tableSchema;

    public CassandraDynamicTableSource(CassandraSourceConf sourceConf, TableSchema tableSchema) {
        this.sourceConf = sourceConf;
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

        RowType rowType =
                TableUtil.createRowType(sourceConf.getColumn(), CassandraRawTypeConverter::apply);

        String[] fieldNames = tableSchema.getFieldNames();
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);

        for (int i = 0; i < fieldNames.length; i++) {
            String name = fieldNames[i];
            FieldConf field = new FieldConf();

            field.setName(name);
            field.setIndex(i);

            columnList.add(field);
        }

        sourceConf.setColumn(columnList);

        builder.setSourceConf(sourceConf);
        builder.setRowConverter(new CassandraColumnConverter(rowType));

        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInfo),
                false,
                sourceConf.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new CassandraDynamicTableSource(sourceConf, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return IDENTIFIER;
    }
}
