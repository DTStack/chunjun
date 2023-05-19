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

package com.dtstack.chunjun.connector.redis.source;

import com.dtstack.chunjun.connector.redis.config.RedisConfig;
import com.dtstack.chunjun.connector.redis.converter.RedisSqlConverter;
import com.dtstack.chunjun.connector.redis.lookup.RedisAllTableFunction;
import com.dtstack.chunjun.connector.redis.lookup.RedisLruTableFunction;
import com.dtstack.chunjun.enums.CacheType;
import com.dtstack.chunjun.lookup.config.LookupConfig;
import com.dtstack.chunjun.source.DtInputFormatSourceFunction;
import com.dtstack.chunjun.table.connector.source.ParallelAsyncLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelLookupFunctionProvider;
import com.dtstack.chunjun.table.connector.source.ParallelSourceFunctionProvider;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

public class RedisDynamicTableSource
        implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown {

    protected ResolvedSchema physicalSchema;
    protected final RedisConfig redisConfig;
    protected final LookupConfig lookupConfig;

    public RedisDynamicTableSource(
            ResolvedSchema physicalSchema, RedisConfig redisConfig, LookupConfig lookupConfig) {
        this.physicalSchema = physicalSchema;
        this.redisConfig = redisConfig;
        this.lookupConfig = lookupConfig;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "redis only support non-nested look up keys");
            keyNames[i] = physicalSchema.getColumnNames().get(innerKeyArr[0]);
        }
        LogicalType logicalType = physicalSchema.toPhysicalRowDataType().getLogicalType();
        if (lookupConfig.getCache().equalsIgnoreCase(CacheType.ALL.toString())) {
            return ParallelLookupFunctionProvider.of(
                    new RedisAllTableFunction(
                            redisConfig,
                            lookupConfig,
                            physicalSchema.getColumnNames().toArray(new String[0]),
                            keyNames,
                            new RedisSqlConverter(InternalTypeInfo.of(logicalType).toRowType())),
                    lookupConfig.getParallelism());
        }
        return ParallelAsyncLookupFunctionProvider.of(
                new RedisLruTableFunction(
                        redisConfig,
                        lookupConfig,
                        new RedisSqlConverter(InternalTypeInfo.of(logicalType).toRowType())),
                lookupConfig.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(physicalSchema, redisConfig, lookupConfig);
    }

    @Override
    public String asSummaryString() {
        return "redis";
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        RedisInputFormatBuilder builder = new RedisInputFormatBuilder();
        LogicalType logicalType = physicalSchema.toPhysicalRowDataType().getLogicalType();
        TypeInformation<RowData> typeInformation = InternalTypeInfo.of(logicalType);
        builder.setRedisConf(redisConfig);
        builder.setRowConverter(
                new RedisSqlConverter(InternalTypeInfo.of(logicalType).toRowType()));
        return ParallelSourceFunctionProvider.of(
                new DtInputFormatSourceFunction<>(builder.finish(), typeInformation),
                false,
                redisConfig.getParallelism());
    }
}
