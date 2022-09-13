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

package com.dtstack.chunjun.connector.s3.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.connector.s3.conf.S3Conf;
import com.dtstack.chunjun.connector.s3.converter.S3RowConverter;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

public class S3DynamicTableSink implements DynamicTableSink {
    private final TableSchema schema;
    private final S3Conf s3Conf;

    public S3DynamicTableSink(TableSchema schema, S3Conf s3Conf) {
        this.schema = schema;
        this.s3Conf = s3Conf;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) schema.toRowDataType().getLogicalType();

        String[] fieldNames = schema.getFieldNames();
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            FieldConf field = new FieldConf();
            field.setName(fieldNames[i]);
            field.setType(rowType.getTypeAt(i).asSummaryString());
            field.setIndex(i);
            columnList.add(field);
        }
        s3Conf.setColumn(columnList);
        S3OutputFormatBuilder builder = new S3OutputFormatBuilder(new S3OutputFormat());
        builder.setS3Conf(s3Conf);
        builder.setRowConverter(new S3RowConverter(rowType, s3Conf));

        return SinkFunctionProvider.of(new DtOutputFormatSinkFunction<>(builder.finish()), 1);
    }

    @Override
    public DynamicTableSink copy() {
        return new S3DynamicTableSink(schema, s3Conf);
    }

    @Override
    public String asSummaryString() {
        return "StreamDynamicTableSink";
    }
}
