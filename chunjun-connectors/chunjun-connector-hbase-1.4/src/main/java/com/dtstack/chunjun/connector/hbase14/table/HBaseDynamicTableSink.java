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
package com.dtstack.chunjun.connector.hbase14.table;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.conf.HBaseConf;
import com.dtstack.chunjun.connector.hbase14.converter.HbaseRowConverter;
import com.dtstack.chunjun.connector.hbase14.sink.HBaseOutputFormatBuilder;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 2021/06/21 Company: www.dtstack.com
 *
 * @author tudou
 */
public class HBaseDynamicTableSink implements DynamicTableSink {

    private final HBaseConf conf;
    private final TableSchema tableSchema;
    private final HBaseTableSchema hbaseSchema;
    protected final String nullStringLiteral;

    public HBaseDynamicTableSink(
            HBaseConf conf,
            TableSchema tableSchema,
            HBaseTableSchema hbaseSchema,
            String nullStringLiteral) {
        this.conf = conf;
        this.tableSchema = tableSchema;
        this.hbaseSchema = hbaseSchema;
        this.nullStringLiteral = nullStringLiteral;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
        String[] fieldNames = tableSchema.getFieldNames();
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            FieldConf field = new FieldConf();
            field.setName(fieldNames[i]);
            field.setType(rowType.getTypeAt(i).asSummaryString());
            field.setIndex(i);
            columnList.add(field);
        }

        HBaseOutputFormatBuilder builder = new HBaseOutputFormatBuilder();
        builder.setHbaseConfig(conf.getHbaseConfig());
        builder.setTableName(conf.getTable());
        builder.setWriteBufferSize(conf.getWriteBufferSize());
        HbaseRowConverter hbaseRowConverter = new HbaseRowConverter(hbaseSchema, nullStringLiteral);
        builder.setRowConverter(hbaseRowConverter);
        builder.setConfig(conf);
        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction(builder.finish()), conf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new HBaseDynamicTableSink(conf, tableSchema, hbaseSchema, nullStringLiteral);
    }

    @Override
    public String asSummaryString() {
        return "HbaseDynamicTableSink";
    }
}
