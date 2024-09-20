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

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SpeedConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.s3.config.S3Config;
import com.dtstack.chunjun.connector.s3.converter.S3SqlConverter;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.List;

public class S3DynamicTableSink implements DynamicTableSink {
    private final ResolvedSchema schema;
    private final S3Config s3Config;

    public S3DynamicTableSink(ResolvedSchema schema, S3Config s3Config) {
        this.schema = schema;
        this.s3Config = s3Config;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return requestedMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        LogicalType logicalType = schema.toPhysicalRowDataType().getLogicalType();

        List<Column> columns = schema.getColumns();
        List<FieldConfig> columnList = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            FieldConfig field = new FieldConfig();
            field.setName(column.getName());
            field.setType(
                    TypeConfig.fromString(column.getDataType().getLogicalType().asSummaryString()));
            field.setIndex(i);
            columnList.add(field);
        }
        s3Config.setColumn(columnList);
        S3OutputFormatBuilder builder = new S3OutputFormatBuilder(new S3OutputFormat());
        builder.setSpeedConf(new SpeedConfig());
        builder.setS3Conf(s3Config);
        builder.setRowConverter(
                new S3SqlConverter(InternalTypeInfo.of(logicalType).toRowType(), s3Config));

        int sinkParallelism = s3Config.getParallelism() == null ? 1 : s3Config.getParallelism();
        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), sinkParallelism);
    }

    @Override
    public DynamicTableSink copy() {
        return new S3DynamicTableSink(schema, s3Config);
    }

    @Override
    public String asSummaryString() {
        return S3DynamicTableSink.class.getName();
    }
}
