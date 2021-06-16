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

package com.dtstack.flinkx.connector.emqx.sink;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.emqx.conf.EmqxConf;
import com.dtstack.flinkx.connector.emqx.converter.EmqxRowConverter;
import com.dtstack.flinkx.streaming.api.functions.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chuixue
 * @create 2021-06-01 20:11
 * @description
 */
public class EmqxDynamicTableSink implements DynamicTableSink {

    private final TableSchema physicalSchema;
    private final EmqxConf emqxConf;

    public EmqxDynamicTableSink(TableSchema physicalSchema, EmqxConf emqxConf) {
        this.physicalSchema = physicalSchema;
        this.emqxConf = emqxConf;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        EmqxOutputFormatBuilder builder = new EmqxOutputFormatBuilder();
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
        builder.setConverter(new EmqxRowConverter(rowType));

        return SinkFunctionProvider.of(new DtOutputFormatSinkFunction<>(builder.finish()),
                1);
    }

    @Override
    public DynamicTableSink copy() {
        return new EmqxDynamicTableSink(physicalSchema, emqxConf);
    }

    @Override
    public String asSummaryString() {
        return "EMQX sink";
    }
}
