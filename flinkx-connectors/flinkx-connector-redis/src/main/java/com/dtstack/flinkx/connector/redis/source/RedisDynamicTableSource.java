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

package com.dtstack.flinkx.connector.redis.source;

import com.dtstack.flinkx.connector.redis.conf.RedisConf;
import com.dtstack.flinkx.connector.redis.converter.RedisRowConverter;
import com.dtstack.flinkx.connector.redis.lookup.RedisAllTableFunction;
import com.dtstack.flinkx.connector.redis.lookup.RedisLruTableFunction;
import com.dtstack.flinkx.enums.CacheType;
import com.dtstack.flinkx.lookup.conf.LookupConf;
import com.dtstack.flinkx.table.connector.source.ParallelAsyncTableFunctionProvider;
import com.dtstack.flinkx.table.connector.source.ParallelTableFunctionProvider;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

/**
 * @author chuixue
 * @create 2021-06-21 19:08
 * @description
 */
public class RedisDynamicTableSource implements LookupTableSource, SupportsProjectionPushDown {

    protected TableSchema physicalSchema;
    protected final RedisConf redisConf;
    protected final LookupConf lookupConf;

    public RedisDynamicTableSource(
            TableSchema physicalSchema, RedisConf redisConf, LookupConf lookupConf) {
        this.physicalSchema = physicalSchema;
        this.redisConf = redisConf;
        this.lookupConf = lookupConf;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1, "redis only support non-nested look up keys");
            keyNames[i] = physicalSchema.getFieldNames()[innerKeyArr[0]];
        }
        final RowType rowType = (RowType) physicalSchema.toRowDataType().getLogicalType();

        if (lookupConf.getCache().equalsIgnoreCase(CacheType.LRU.toString())) {
            return ParallelAsyncTableFunctionProvider.of(
                    new RedisLruTableFunction(
                            redisConf, lookupConf, new RedisRowConverter(rowType)),
                    lookupConf.getParallelism());
        }
        return ParallelTableFunctionProvider.of(
                new RedisAllTableFunction(
                        redisConf,
                        lookupConf,
                        physicalSchema.getFieldNames(),
                        keyNames,
                        new RedisRowConverter(rowType)),
                lookupConf.getParallelism());
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(physicalSchema, redisConf, lookupConf);
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
    public void applyProjection(int[][] projectedFields) {
        this.physicalSchema = TableSchemaUtils.projectSchema(physicalSchema, projectedFields);
    }
}
