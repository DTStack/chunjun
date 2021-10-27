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
package com.dtstack.flinkx.connector.hbase14.table;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.hbase.HBaseConverter;
import com.dtstack.flinkx.connector.hbase.HBaseMutationConverter;
import com.dtstack.flinkx.connector.hbase.HBaseTableSchema;
import com.dtstack.flinkx.connector.hbase.RowDataToMutationConverter;
import com.dtstack.flinkx.connector.hbase14.conf.HBaseConf;
import com.dtstack.flinkx.connector.hbase14.sink.HBaseOutputFormatBuilder;
import com.dtstack.flinkx.converter.AbstractRowConverter;
import com.dtstack.flinkx.sink.DtOutputFormatSinkFunction;

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
        if (conf.getColumn() != null) {
            builder.setColumnMetaInfos(conf.getColumn());
        } else if (conf.getColumnMetaInfos() != null) {
            builder.setColumnMetaInfos(conf.getColumnMetaInfos());
        } else if (!columnList.isEmpty()) {
            builder.setColumnMetaInfos(columnList);
        }
        builder.setEncoding(conf.getEncoding());
        builder.setHbaseConfig(conf.getHbaseConfig());
        builder.setNullMode(conf.getNullMode());
        builder.setTableName(conf.getTableName());
        builder.setRowkeyExpress(conf.getRowkeyExpress());
        builder.setVersionColumnIndex(conf.getVersionColumnIndex());
        builder.setVersionColumnValues(conf.getVersionColumnValue());
        builder.setWalFlag(conf.getWalFlag());
        builder.setWriteBufferSize(conf.getWriteBufferSize());
        AbstractRowConverter rowConverter = new HBaseConverter(rowType);
        builder.setRowConverter(rowConverter);
        builder.setConfig(conf);

        HBaseMutationConverter converter =
                new RowDataToMutationConverter(hbaseSchema, conf.getNullMode());
        builder.setHBaseMutationConverter(converter);

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
