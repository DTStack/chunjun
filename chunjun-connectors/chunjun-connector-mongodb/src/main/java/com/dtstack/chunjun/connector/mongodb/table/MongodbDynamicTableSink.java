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

package com.dtstack.chunjun.connector.mongodb.table;

import com.dtstack.chunjun.config.CommonConfig;
import com.dtstack.chunjun.connector.mongodb.config.MongoClientConfig;
import com.dtstack.chunjun.connector.mongodb.config.MongoWriteConfig;
import com.dtstack.chunjun.connector.mongodb.converter.MongodbSqlConverter;
import com.dtstack.chunjun.connector.mongodb.sink.MongodbOutputFormat;
import com.dtstack.chunjun.connector.mongodb.sink.MongodbOutputFormatBuilder;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

public class MongodbDynamicTableSink implements DynamicTableSink {

    private final MongoClientConfig mongoClientConfig;
    private final ResolvedSchema resolvedSchema;
    private final MongoWriteConfig mongoWriteConfig;

    public MongodbDynamicTableSink(
            MongoClientConfig mongoClientConfig,
            ResolvedSchema resolvedSchema,
            MongoWriteConfig mongoWriteConfig) {
        this.mongoClientConfig = mongoClientConfig;
        this.resolvedSchema = resolvedSchema;
        this.mongoWriteConfig = mongoWriteConfig;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final RowType rowType = (RowType) resolvedSchema.toPhysicalRowDataType().getLogicalType();
        String[] fieldNames = resolvedSchema.getColumnNames().toArray(new String[0]);
        MongodbOutputFormatBuilder builder =
                new MongodbOutputFormatBuilder(
                        null, mongoClientConfig, null, MongodbOutputFormat.WriteMode.INSERT);
        CommonConfig commonConfig = new CommonConfig();
        commonConfig.setBatchSize(mongoWriteConfig.getFlushMaxRows());
        commonConfig.setFlushIntervalMills(mongoWriteConfig.getFlushInterval());
        builder.setConfig(commonConfig);

        builder.setRowConverter(new MongodbSqlConverter(rowType, fieldNames));
        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()),
                mongoWriteConfig.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new MongodbDynamicTableSink(mongoClientConfig, resolvedSchema, mongoWriteConfig);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB Sink";
    }
}
