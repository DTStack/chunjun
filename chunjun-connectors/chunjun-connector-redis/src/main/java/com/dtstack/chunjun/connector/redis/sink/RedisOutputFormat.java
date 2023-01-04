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
import com.dtstack.chunjun.connector.redis.connection.RedisSyncClient;
import com.dtstack.chunjun.sink.format.BaseRichOutputFormat;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;

@Slf4j
public class RedisOutputFormat extends BaseRichOutputFormat {

    private static final long serialVersionUID = -5545866738532370105L;

    private transient RedisSyncClient redisSyncClient;
    /** redis Conf */
    private RedisConfig redisConfig;
    /** jedis */
    private JedisCommands jedis;

    private static final String TEST_KEY = "test";

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        redisSyncClient = new RedisSyncClient(redisConfig);
        jedis = redisSyncClient.getJedis();
    }

    @Override
    protected void writeSingleRecordInternal(RowData rowData) throws WriteRecordException {
        try {
            writeSingleRecordWithRetry(rowData);
        } catch (Exception e) {
            throw new WriteRecordException("writer data error", e, 0, rowData);
        }
    }

    private void writeSingleRecordWithRetry(RowData rowData) throws Exception {
        try {
            rowConverter.toExternal(rowData, jedis);
        } catch (JedisConnectionException e) {
            // JedisConnectionException may be caused by jedis time out ,retry to get jedis from
            // pool
            log.error("retry get redis once");
            jedis = redisSyncClient.testTimeout(jedis, TEST_KEY);
            rowConverter.toExternal(rowData, jedis);
        }
    }

    @Override
    protected void writeMultipleRecordsInternal() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void closeInternal() {
        redisSyncClient.close(jedis);
    }

    public RedisConfig getRedisConf() {
        return redisConfig;
    }

    public void setRedisConf(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }
}
