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
import com.dtstack.chunjun.connector.redis.connection.RedisSyncClient;
import com.dtstack.chunjun.connector.redis.inputsplit.RedisInputSplit;
import com.dtstack.chunjun.connector.redis.util.RedisUtil;
import com.dtstack.chunjun.source.format.BaseRichInputFormat;
import com.dtstack.chunjun.throwable.ReadRecordException;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.table.data.RowData;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.commands.JedisCommands;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisInputFormat extends BaseRichInputFormat {
    private static final long serialVersionUID = 88062910932514147L;
    private transient RedisSyncClient redisSyncClient;

    private transient Iterator<String> keyIterator;
    private RedisConfig redisConfig;
    private JedisCommands jedis;

    @Override
    protected InputSplit[] createInputSplitsInternal(int minNumSplits) {
        redisSyncClient = new RedisSyncClient(redisConfig);
        jedis = redisSyncClient.getJedis();
        Set<String> keys = Sets.newHashSet();
        if (StringUtils.isNotBlank(redisConfig.getKeyPrefix())) {
            keys.addAll(
                    RedisUtil.getRedisKeys(
                            redisConfig.getRedisConnectType(), jedis, redisConfig.getKeyPrefix()));
        }
        Iterator<String> iterator = keys.iterator();
        RedisInputSplit[] inputSplits = new RedisInputSplit[minNumSplits];
        if (keys.size() == 0) {
            throw new RuntimeException("There is no" + redisConfig.getKeyPrefix() + "with key");
        }
        int keySplitCount = keys.size() / minNumSplits;
        for (int i = 0; i < inputSplits.length; i++) {
            List<String> list = Lists.newLinkedList();
            for (int j = 0; j < keySplitCount && iterator.hasNext(); j++) {
                list.add(iterator.next());
            }
            inputSplits[i] = new RedisInputSplit(i, minNumSplits, list);
        }
        while (iterator.hasNext()) {
            inputSplits[minNumSplits - 1].getKey().add(iterator.next());
        }
        redisSyncClient.close(jedis);
        return inputSplits;
    }

    @Override
    protected void openInternal(InputSplit inputSplit) {
        RedisInputSplit redisInputSplit = (RedisInputSplit) inputSplit;
        redisSyncClient = new RedisSyncClient(redisConfig);
        jedis = redisSyncClient.getJedis();
        keyIterator = redisInputSplit.getKey().iterator();
    }

    @Override
    protected RowData nextRecordInternal(RowData rowData) throws ReadRecordException {
        if (!keyIterator.hasNext()) {
            return null;
        }
        Map<String, String> map = jedis.hgetAll(keyIterator.next());
        try {
            return rowConverter.toInternal(map);
        } catch (Exception e) {
            throw new ReadRecordException("", e, 0, rowData);
        }
    }

    @Override
    protected void closeInternal() {
        redisSyncClient.close(jedis);
    }

    @Override
    public boolean reachedEnd() {
        return !keyIterator.hasNext();
    }

    public RedisConfig getRedisConf() {
        return redisConfig;
    }

    public void setRedisConf(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }
}
