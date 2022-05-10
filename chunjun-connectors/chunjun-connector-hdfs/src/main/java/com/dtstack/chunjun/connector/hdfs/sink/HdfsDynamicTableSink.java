/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.hdfs.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.hdfs.conf.HdfsConf;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsOrcRowConverter;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsParquetRowConverter;
import com.dtstack.chunjun.connector.hdfs.converter.HdfsTextRowConverter;
import com.dtstack.chunjun.connector.hdfs.enums.FileType;
import com.dtstack.chunjun.converter.AbstractRowConverter;
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
public class HdfsDynamicTableSink implements DynamicTableSink {

    private final HdfsConf hdfsConf;
    private final TableSchema tableSchema;

    public HdfsDynamicTableSink(HdfsConf hdfsConf, TableSchema tableSchema) {
        this.hdfsConf = hdfsConf;
        this.tableSchema = tableSchema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    @SuppressWarnings("all")
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
        hdfsConf.setColumn(columnList);
        HdfsOutputFormatBuilder builder = new HdfsOutputFormatBuilder(hdfsConf.getFileType());
        builder.setHdfsConf(hdfsConf);
        AbstractRowConverter rowConverter;
        switch (FileType.getByName(hdfsConf.getFileType())) {
            case ORC:
                rowConverter = new HdfsOrcRowConverter(rowType);
                break;
            case PARQUET:
                rowConverter = new HdfsParquetRowConverter(rowType);
                break;
            default:
                rowConverter = new HdfsTextRowConverter(rowType);
        }
        builder.setRowConverter(rowConverter);
        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction(builder.finish()), hdfsConf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new HdfsDynamicTableSink(hdfsConf, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "HdfsDynamicTableSink";
    }
}
