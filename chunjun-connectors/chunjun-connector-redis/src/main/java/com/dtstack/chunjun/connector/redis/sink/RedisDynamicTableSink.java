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

package com.dtstack.chunjun.connector.redis.sink;

import com.dtstack.chunjun.connector.redis.config.RedisConfig;
import com.dtstack.chunjun.connector.redis.converter.RedisRowConverter;
import com.dtstack.chunjun.sink.DtOutputFormatSinkFunction;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import com.google.common.collect.Lists;

public class RedisDynamicTableSink implements DynamicTableSink {

    private final ResolvedSchema resolvedSchema;
    private final RedisConfig redisConfig;

    public RedisDynamicTableSink(ResolvedSchema resolvedSchema, RedisConfig redisConfig) {
        this.resolvedSchema = resolvedSchema;
        this.redisConfig = redisConfig;
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
        LogicalType logicalType = resolvedSchema.toPhysicalRowDataType().getLogicalType();

        RedisOutputFormatBuilder builder = new RedisOutputFormatBuilder();
        redisConfig.setKeyIndexes(Lists.newArrayList(1));
        builder.setRedisConf(redisConfig);
        builder.setRowConverter(
                new RedisRowConverter(InternalTypeInfo.of(logicalType).toRowType(), redisConfig));

        return SinkFunctionProvider.of(
                new DtOutputFormatSinkFunction<>(builder.finish()), redisConfig.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(resolvedSchema, redisConfig);
    }

    @Override
    public String asSummaryString() {
        return "redis";
    }
}
