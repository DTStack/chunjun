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

package com.dtstack.chunjun.connector.redis.util;

import com.dtstack.chunjun.connector.redis.enums.RedisConnectType;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** @Author OT @Date 2022/7/27 */
public class RedisUtil {
    public static Set<String> getRedisKeys(
            RedisConnectType redisType, JedisCommands jedis, String keyPattern) {
        if (!redisType.equals(RedisConnectType.CLUSTER)) {
            return ((Jedis) jedis).keys(keyPattern);
        }
        Set<String> keys = new TreeSet<>();
        Map<String, JedisPool> clusterNodes = ((JedisCluster) jedis).getClusterNodes();
        for (String k : clusterNodes.keySet()) {
            JedisPool jp = clusterNodes.get(k);
            try (Jedis connection = jp.getResource()) {
                keys.addAll(connection.keys(keyPattern));
            } catch (Exception e) {
                throw new RuntimeException("Getting keys error", e);
            }
        }
        return keys;
    }
}
