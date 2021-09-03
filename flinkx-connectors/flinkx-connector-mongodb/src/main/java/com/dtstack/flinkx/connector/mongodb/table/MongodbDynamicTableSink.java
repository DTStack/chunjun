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

package com.dtstack.flinkx.connector.mongodb.table;

import com.dtstack.flinkx.conf.FlinkxCommonConf;
import com.dtstack.flinkx.connector.mongodb.conf.MongoClientConf;
import com.dtstack.flinkx.connector.mongodb.conf.MongoWriteConf;
import com.dtstack.flinkx.connector.mongodb.converter.MongodbRowConverter;
import com.dtstack.flinkx.connector.mongodb.sink.MongodbOutputFormat;
import com.dtstack.flinkx.connector.mongodb.sink.MongodbOutputFormatBuilder;
import com.dtstack.flinkx.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/21
 */
public class MongodbDynamicTableSink implements DynamicTableSink {

    private final MongoClientConf mongoClientConf;
    private final TableSchema physicalSchema;
    private final MongoWriteConf mongoWriteConf;

    public MongodbDynamicTableSink(
            MongoClientConf mongoClientConf,
            TableSchema physicalSchema,
            MongoWriteConf mongoWriteConf) {
        this.mongoClientConf = mongoClientConf;
        this.physicalSchema = physicalSchema;
        this.mongoWriteConf = mongoWriteConf;
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
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();
        String[] fieldNames = physicalSchema.getFieldNames();
        MongodbOutputFormatBuilder builder =
                new MongodbOutputFormatBuilder(
                        mongoClientConf, null, MongodbOutputFormat.WriteMode.INSERT);
        FlinkxCommonConf flinkxCommonConf = new FlinkxCommonConf();
        flinkxCommonConf.setBatchSize(mongoWriteConf.getFlushMaxRows());
        flinkxCommonConf.setFlushIntervalMills(mongoWriteConf.getFlushInterval());
        builder.setConfig(flinkxCommonConf);

        builder.setRowConverter(new MongodbRowConverter(rowType, fieldNames));
        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction(builder.finish()), mongoWriteConf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new MongodbDynamicTableSink(mongoClientConf, physicalSchema, mongoWriteConf);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB Sink";
    }
}
