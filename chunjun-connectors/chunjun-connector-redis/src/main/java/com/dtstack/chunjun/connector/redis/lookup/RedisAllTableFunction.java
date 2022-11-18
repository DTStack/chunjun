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
import com.dtstack.chunjun.connector.redis.connection.RedisSyncClient;
import com.dtstack.chunjun.connector.redis.util.RedisUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConfig;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import redis.clients.jedis.commands.JedisCommands;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class RedisAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = -1319108555631638546L;

    private final RedisConfig redisConfig;
    private transient RedisSyncClient redisSyncClient;

    public RedisAllTableFunction(
            RedisConfig redisConfig,
            LookupConfig lookupConfig,
            String[] fieldNames,
            String[] keyNames,
            AbstractRowConverter rowConverter) {
        super(fieldNames, keyNames, lookupConfig, rowConverter);
        this.redisConfig = redisConfig;
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        List<String> dataList = Lists.newLinkedList();
        List<RowData> hitRowData = Lists.newArrayList();
        for (int i = 0; i < keyRow.getArity(); i++) {
            dataList.add(String.valueOf(fieldGetters[i].getFieldOrNull(keyRow)));
        }
        String cacheKey = redisConfig.getTableName() + "_" + String.join("_", dataList);
        List<Map<String, Object>> cacheList =
                ((Map<String, List<Map<String, Object>>>) (cacheRef.get())).get(cacheKey);
        // 有数据才往下发，(左/内)连接flink会做相应的处理
        if (!CollectionUtils.isEmpty(cacheList)) {
            cacheList.forEach(one -> hitRowData.add(fillData(one)));
        }

        return hitRowData;
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache =
                (Map<String, List<Map<String, Object>>>) cacheRef;
        if (redisSyncClient == null) {
            redisSyncClient = new RedisSyncClient(redisConfig);
        }
        JedisCommands jedis = redisSyncClient.getJedis();
        StringBuilder keyPattern = new StringBuilder(redisConfig.getTableName());
        for (int i = 0; i < keyNames.length; i++) {
            keyPattern.append("_").append("*");
        }

        Set<String> keys =
                RedisUtil.getRedisKeys(
                        redisConfig.getRedisConnectType(), jedis, keyPattern.toString());
        if (CollectionUtils.isEmpty(keys)) {
            return;
        }

        try {
            for (String key : keys) {
                Map<String, Object> oneRow = new HashMap<>();
                Map<String, String> hgetAll = jedis.hgetAll(key);
                // 防止一条数据有问题，后面数据无法加载
                try {
                    GenericRowData rowData = (GenericRowData) rowConverter.toInternal(hgetAll);
                    for (int i = 0; i < fieldsName.length; i++) {
                        Object object = rowData.getField(i);
                        oneRow.put(fieldsName[i].trim(), object);
                    }
                    tmpCache.computeIfAbsent(key, k -> Lists.newArrayList()).add(oneRow);
                } catch (Exception e) {
                    log.error("error:{} \n  data:{}", e.getMessage(), hgetAll);
                }
            }
        } catch (Exception e) {
            log.error("", e);
        } finally {
            redisSyncClient.closeJedis(jedis);
        }
    }
}
