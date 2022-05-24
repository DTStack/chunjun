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

import com.dtstack.chunjun.connector.hbase.HBaseTableSchema;
import com.dtstack.chunjun.connector.hbase.conf.HBaseConf;
import com.dtstack.chunjun.connector.hbase14.converter.HbaseRowConverter;
import com.dtstack.chunjun.connector.hbase14.sink.HBaseOutputFormatBuilder;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;

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

    public HBaseDynamicTableSink(
            HBaseConf conf, TableSchema tableSchema, HBaseTableSchema hbaseSchema) {
        this.conf = conf;
        this.tableSchema = tableSchema;
        this.hbaseSchema = hbaseSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        List<LogicalType> logicalTypes = new ArrayList<>();

        String[] familyNames = hbaseSchema.getFamilyNames();
        int rowKeyIndex = hbaseSchema.getRowKeyIndex();
        for (int i = 0; i < familyNames.length; i++) {
            if (i == rowKeyIndex) {
                logicalTypes.add(
                        TypeConversions.fromDataToLogicalType(
                                hbaseSchema.getRowKeyDataType().get()));
            }
            DataType[] qualifierDataTypes = hbaseSchema.getQualifierDataTypes(familyNames[i]);
            for (DataType dataType : qualifierDataTypes) {
                logicalTypes.add(TypeConversions.fromDataToLogicalType(dataType));
            }
        }

        // todo 测试下顺序是否是一致的
        RowType of = RowType.of(logicalTypes.toArray(new LogicalType[0]));

        HBaseOutputFormatBuilder builder = new HBaseOutputFormatBuilder();
        builder.setConfig(conf);
        builder.setHbaseConf(conf);
        builder.setHbaseConfig(conf.getHbaseConfig());
        builder.setTableName(conf.getTable());

        builder.setWriteBufferSize(conf.getWriteBufferSize());
        String nullStringLiteral = conf.getNullStringLiteral();

        AbstractRowConverter rowConverter = new HbaseRowConverter(hbaseSchema, nullStringLiteral);
        builder.setRowConverter(rowConverter);

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction(builder.finish()), conf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new HBaseDynamicTableSink(conf, tableSchema, hbaseSchema);
    }

    @Override
    public String asSummaryString() {
        return "HbaseDynamicTableSink";
    }
}
