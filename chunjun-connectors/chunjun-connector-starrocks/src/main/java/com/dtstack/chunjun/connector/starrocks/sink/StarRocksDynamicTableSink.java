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

package com.dtstack.chunjun.connector.starrocks.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.starrocks.conf.StarRocksConf;
import com.dtstack.chunjun.connector.starrocks.converter.StarRocksRowConverter;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/** @author liuliu 2022/7/12 */
public class StarRocksDynamicTableSink implements DynamicTableSink {
    private final StarRocksConf sinkConf;
    private final TableSchema physicalSchema;

    public StarRocksDynamicTableSink(StarRocksConf sinkConf, TableSchema physicalSchema) {
        this.sinkConf = sinkConf;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        StarRocksOutputFormatBuilder builder =
                new StarRocksOutputFormatBuilder(new StarRocksOutputFormat());

        sinkConf.setColumn(getFieldConfFromSchema());
        builder.setRowConverter(
                new StarRocksRowConverter(rowType, Arrays.asList(physicalSchema.getFieldNames())));
        builder.setStarRocksConf(sinkConf);

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), sinkConf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new StarRocksDynamicTableSink(sinkConf, physicalSchema);
    }

    @Override
    public String asSummaryString() {
        return "StarRocks Sink";
    }

    private List<FieldConf> getFieldConfFromSchema() {

        return physicalSchema.getTableColumns().stream()
                .map(
                        tableColumn -> {
                            FieldConf fieldConf = new FieldConf();
                            fieldConf.setName(tableColumn.getName());
                            fieldConf.setType(tableColumn.getType().getConversionClass().getName());
                            return fieldConf;
                        })
                .collect(Collectors.toList());
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(
                ChangelogMode.insertOnly().equals(requestedMode)
                        || physicalSchema.getPrimaryKey().isPresent(),
                "please declare primary key for sink table when query contains update/delete record.");
    }
}
