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

import com.dtstack.chunjun.connector.redis.conf.RedisConf;
import com.dtstack.chunjun.connector.redis.connection.RedisSyncClient;
import com.dtstack.chunjun.connector.redis.util.RedisUtil;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.lookup.AbstractAllTableFunction;
import com.dtstack.chunjun.lookup.config.LookupConf;

import org.apache.flink.table.data.GenericRowData;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCommands;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author chuixue
 * @create 2021-06-16 15:17
 * @description
 */
public class RedisAllTableFunction extends AbstractAllTableFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RedisAllTableFunction.class);
    private final RedisConf redisConf;
    private transient RedisSyncClient redisSyncClient;

    public RedisAllTableFunction(
            RedisConf redisConf,
            LookupConf lookupConf,
            String[] fieldNames,
            String[] keyNames,
            AbstractRowConverter rowConverter) {
        super(fieldNames, keyNames, lookupConf, rowConverter);
        this.redisConf = redisConf;
    }

    @Override
    public void eval(Object... keys) {
        String keyPattern =
                redisConf.getTableName()
                        + "_"
                        + Arrays.stream(keys).map(String::valueOf).collect(Collectors.joining("_"));
        List<Map<String, Object>> cacheList =
                ((Map<String, List<Map<String, Object>>>) cacheRef.get()).get(keyPattern);

        // 有数据才往下发，(左/内)连接flink会做相应的处理
        if (!CollectionUtils.isEmpty(cacheList)) {
            cacheList.forEach(one -> collect(fillData(one)));
        }
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache =
                (Map<String, List<Map<String, Object>>>) cacheRef;
        if (redisSyncClient == null) {
            redisSyncClient = new RedisSyncClient(redisConf);
        }
        JedisCommands jedis = redisSyncClient.getJedis();
        StringBuilder keyPattern = new StringBuilder(redisConf.getTableName());
        for (int i = 0; i < keyNames.length; i++) {
            keyPattern.append("_").append("*");
        }

        Set<String> keys =
                RedisUtil.getRedisKeys(
                        redisConf.getRedisConnectType(), jedis, keyPattern.toString());
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
                    LOG.error("error:{} \n  data:{}", e.getMessage(), hgetAll);
                }
            }
        } catch (Exception e) {
            LOG.error("", e);
        } finally {
            redisSyncClient.closeJedis(jedis);
        }
    }
}
