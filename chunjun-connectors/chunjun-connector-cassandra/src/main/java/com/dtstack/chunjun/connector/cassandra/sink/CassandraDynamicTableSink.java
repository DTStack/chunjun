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

package com.dtstack.chunjun.connector.cassandra.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.cassandra.config.CassandraSinkConfig;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraRawTypeConverter;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraSqlConverter;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@AllArgsConstructor
public class CassandraDynamicTableSink implements DynamicTableSink {

    private static final String IDENTIFIED = "Cassandra";

    private final CassandraSinkConfig sinkConf;

    private final ResolvedSchema tableSchema;

    private final DataType dataType;

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        // TODO upsert ? append ? update ?
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        CassandraOutputFormatBuilder builder = new CassandraOutputFormatBuilder();

        List<Column> columns = tableSchema.getColumns();

        List<FieldConfig> columnList = new ArrayList<>(columns.size());
        List<String> columnNameList = new ArrayList<>();

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

        sinkConf.setColumn(columnList);

        final RowType rowType =
                TableUtil.createRowType(sinkConf.getColumn(), CassandraRawTypeConverter::apply);

        builder.setSinkConfig(sinkConf);
        builder.setRowConverter(new CassandraSqlConverter(rowType, columnNameList));

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), sinkConf.getParallelism());
    }

    @Override
    public String asSummaryString() {
        return IDENTIFIED;
    }

    @Override
    public DynamicTableSink copy() {
        return new CassandraDynamicTableSink(sinkConf, tableSchema, dataType);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof CassandraDynamicTableSink)) {
            return false;
        }
        CassandraDynamicTableSink that = (CassandraDynamicTableSink) o;
        return Objects.equals(sinkConf, that.sinkConf)
                && Objects.equals(tableSchema, that.tableSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sinkConf, tableSchema);
    }
}
