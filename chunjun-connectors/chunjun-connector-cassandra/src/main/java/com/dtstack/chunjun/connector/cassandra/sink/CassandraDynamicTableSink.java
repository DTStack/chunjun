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

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.cassandra.conf.CassandraSinkConf;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraRawTypeConverter;
import com.dtstack.chunjun.connector.cassandra.converter.CassandraRowConverter;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraDynamicTableSink implements DynamicTableSink {

    private static final String IDENTIFIED = "Cassandra";

    private final CassandraSinkConf sinkConf;

    private final TableSchema tableSchema;

    public CassandraDynamicTableSink(CassandraSinkConf sinkConf, TableSchema tableSchema) {
        this.sinkConf = sinkConf;
        this.tableSchema = tableSchema;
    }

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

        sinkConf.setColumn(columnList);

        final RowType rowType =
                TableUtil.createRowType(sinkConf.getColumn(), CassandraRawTypeConverter::apply);

        builder.setSinkConf(sinkConf);
        builder.setRowConverter(new CassandraRowConverter(rowType, columnNameList));

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction(builder.finish()), sinkConf.getParallelism());
    }

    @Override
    public String asSummaryString() {
        return IDENTIFIED;
    }

    @Override
    public DynamicTableSink copy() {
        return new CassandraDynamicTableSink(sinkConf, tableSchema);
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
