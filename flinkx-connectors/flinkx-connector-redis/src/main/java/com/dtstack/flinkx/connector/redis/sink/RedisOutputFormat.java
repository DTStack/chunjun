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

package com.dtstack.flinkx.connector.redis.sink;

import com.dtstack.flinkx.connector.redis.conf.RedisConf;
import com.dtstack.flinkx.connector.redis.connection.RedisSyncClient;
import com.dtstack.flinkx.sink.format.BaseRichOutputFormat;
import com.dtstack.flinkx.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;

/**
 * @author chuixue
 * @create 2021-06-16 15:12
 * @description
 */
public class RedisOutputFormat extends BaseRichOutputFormat {

    private transient RedisSyncClient redisSyncClient;
    /** redis Conf */
    private RedisConf redisConf;
    /** jedis */
    private JedisCommands jedis;

    private String TEST_KEY = "test";

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        redisSyncClient = new RedisSyncClient(redisConf);
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

    /**
     * insert data and jedis retry
     *
     * @param rowData
     * @throws Exception
     */
    private void writeSingleRecordWithRetry(RowData rowData) throws Exception {
        try {
            rowConverter.toExternal(rowData, jedis);
        } catch (JedisConnectionException e) {
            // JedisConnectionException may be caused by jedis time out ,retry to get jedis from
            // pool
            LOG.error("retry get redis once");
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

    public RedisConf getRedisConf() {
        return redisConf;
    }

    public void setRedisConf(RedisConf redisConf) {
        this.redisConf = redisConf;
    }
}
