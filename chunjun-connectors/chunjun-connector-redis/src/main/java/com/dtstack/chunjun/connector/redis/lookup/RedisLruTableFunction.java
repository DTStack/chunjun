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

package com.dtstack.chunjun.connector.redis.lookup;

import com.dtstack.chunjun.connector.redis.config.RedisConfig;
import com.dtstack.chunjun.connector.redis.connection.RedisAsyncClient;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.enums.ECacheContentType;
import com.dtstack.chunjun.lookup.AbstractLruTableFunction;
import com.dtstack.chunjun.lookup.cache.CacheMissVal;
import com.dtstack.chunjun.lookup.cache.CacheObj;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;

import com.google.common.collect.Lists;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class RedisLruTableFunction extends AbstractLruTableFunction {

    private static final long serialVersionUID = -7190171034606939751L;

    private transient RedisAsyncClient redisAsyncClient;
    private RedisKeyAsyncCommands<String, String> redisKeyAsyncCommands;
    private final RedisConfig redisConfig;

    public RedisLruTableFunction(
            RedisConfig redisConfig, LookupConfig lookupConfig, AbstractRowConverter rowConverter) {
        super(lookupConfig, rowConverter);
        this.redisConfig = redisConfig;
        this.lookupConfig = lookupConfig;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        redisAsyncClient = new RedisAsyncClient(redisConfig);
        redisKeyAsyncCommands = redisAsyncClient.getRedisKeyAsyncCommands();
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<RowData>> future, Object... keys) {
        String cacheKey = buildCacheKey(keys);
        RedisFuture<Map<String, String>> resultFuture =
                ((RedisHashAsyncCommands) redisKeyAsyncCommands).hgetall(cacheKey);
        resultFuture.thenAccept(
                resultValues -> {
                    if (MapUtils.isNotEmpty(resultValues)) {
                        List<Map<String, String>> cacheContent = Lists.newArrayList();
                        List<RowData> rowList = Lists.newArrayList();
                        try {
                            RowData rowData = rowConverter.toInternalLookup(resultValues);
                            if (openCache()) {
                                cacheContent.add(resultValues);
                            }
                            rowList.add(rowData);
                        } catch (Exception e) {
                            log.error(
                                    "error:{} \n cacheKey:{} \n data:{}",
                                    e.getMessage(),
                                    cacheKey,
                                    resultValues);
                        }
                        dealCacheData(
                                cacheKey,
                                CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                        future.complete(rowList);
                    } else {
                        dealMissKey(future);
                        dealCacheData(cacheKey, CacheMissVal.getMissKeyObj());
                    }
                });
    }

    @Override
    public String buildCacheKey(Object... keys) {
        return redisConfig.getTableName() + "_" + super.buildCacheKey(keys);
    }

    @Override
    public void close() {
        redisAsyncClient.close();
    }
}
