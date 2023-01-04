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

package com.dtstack.chunjun.connector.redis.connection;

import com.dtstack.chunjun.connector.redis.config.RedisConfig;
import com.dtstack.chunjun.util.ExceptionUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static com.dtstack.chunjun.connector.redis.options.RedisOptions.REDIS_HOST_PATTERN;

@Slf4j
public class RedisSyncClient {

    private JedisPool pool;

    private JedisSentinelPool jedisSentinelPool;

    private final RedisConfig redisConfig;

    public RedisSyncClient(RedisConfig redisConfig) {
        this.redisConfig = redisConfig;
    }

    private JedisCommands getJedisInner() {
        JedisPoolConfig poolConfig = getConfig();
        String[] nodes = StringUtils.split(redisConfig.getHostPort(), ",");

        JedisCommands jedis;
        switch (redisConfig.getRedisConnectType()) {
            case STANDALONE:
                String firstIp = null;
                String firstPort = null;
                Matcher standalone = REDIS_HOST_PATTERN.defaultValue().matcher(nodes[0]);
                if (standalone.find()) {
                    firstIp = standalone.group("host").trim();
                    firstPort = standalone.group("port").trim();
                }
                if (Objects.nonNull(firstIp) && pool == null) {
                    pool =
                            new JedisPool(
                                    poolConfig,
                                    firstIp,
                                    Integer.parseInt(firstPort),
                                    redisConfig.getTimeout(),
                                    redisConfig.getPassword(),
                                    redisConfig.getDatabase());
                } else {
                    throw new IllegalArgumentException(
                            String.format("redis url error. current url [%s]", nodes[0]));
                }

                jedis = pool.getResource();
                break;
            case SENTINEL:
                Set<String> ipPorts = new HashSet<>(Arrays.asList(nodes));
                if (jedisSentinelPool == null) {
                    jedisSentinelPool =
                            new JedisSentinelPool(
                                    redisConfig.getMasterName(),
                                    ipPorts,
                                    poolConfig,
                                    redisConfig.getTimeout(),
                                    redisConfig.getPassword(),
                                    redisConfig.getDatabase());
                }
                jedis = jedisSentinelPool.getResource();
                break;
            case CLUSTER:
                Set<HostAndPort> addresses = new HashSet<>();
                // 对ipv6 支持
                for (String node : nodes) {
                    Matcher matcher = REDIS_HOST_PATTERN.defaultValue().matcher(node);
                    if (matcher.find()) {
                        String host = matcher.group("host").trim();
                        String portStr = matcher.group("port").trim();
                        if (StringUtils.isNotBlank(host) && StringUtils.isNotBlank(portStr)) {
                            // 转化为int格式的端口
                            int port = Integer.parseInt(portStr);
                            addresses.add(new HostAndPort(host, port));
                        }
                    }
                }
                jedis =
                        new JedisCluster(
                                addresses,
                                redisConfig.getTimeout(),
                                redisConfig.getTimeout(),
                                10,
                                redisConfig.getPassword(),
                                getObjectConfig());
                break;
            default:
                throw new IllegalArgumentException(
                        "unsupported redis type[ " + redisConfig.getType().getType() + "]");
        }

        return jedis;
    }

    /**
     * test jedis client whether is in active or not,if not,close and get a new jedis client
     *
     * @param jedis
     * @param key
     * @return
     * @throws IOException
     */
    public JedisCommands testTimeout(JedisCommands jedis, String key) throws IOException {
        try {
            jedis.exists(key);
            return jedis;
        } catch (JedisConnectionException e) {
            if (jedis != null) {
                closeJedis(jedis);
            }
            return pool.getResource();
        }
    }

    public JedisCommands getJedis() {
        JedisCommands jedisInner = null;
        for (int i = 0; i <= 2; i++) {
            try {
                log.info("connect " + (i + 1) + " times.");
                jedisInner = getJedisInner();
                if (jedisInner != null) {
                    log.info("jedis is connected = {} ", jedisInner);
                    break;
                }
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                log.error(
                        "connect failed:{} , sleep 3 seconds reconnect",
                        ExceptionUtil.getErrorMessage(e));
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException interruptedException) {
                    throw new RuntimeException(interruptedException);
                }
                if (i == 2) {
                    throw new RuntimeException(e);
                }
            }
        }
        return jedisInner;
    }

    public void closeJedis(JedisCommands jedis) {
        try {
            if (jedis != null) {
                if (jedis instanceof Closeable) {
                    ((Closeable) jedis).close();
                }
            }
        } catch (Exception e) {
            log.error("close jedis error", e);
        }
    }

    public void close(JedisCommands jedis) {
        try {
            if (jedis != null && ((Jedis) jedis).isConnected()) {
                ((Closeable) jedis).close();
            }
            if (jedisSentinelPool != null) {
                jedisSentinelPool.close();
            }
            if (pool != null) {
                pool.close();
            }
        } catch (Exception e) {
            log.error(ExceptionUtil.getErrorMessage(e));
        }
    }

    private JedisPoolConfig getConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(redisConfig.getMaxTotal());
        jedisPoolConfig.setMaxIdle(redisConfig.getMaxIdle());
        jedisPoolConfig.setMinIdle(redisConfig.getMinIdle());
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        return jedisPoolConfig;
    }

    private GenericObjectPoolConfig<Connection> getObjectConfig() {
        GenericObjectPoolConfig<Connection> jedisPoolConfig = new GenericObjectPoolConfig<>();
        jedisPoolConfig.setMaxTotal(redisConfig.getMaxTotal());
        jedisPoolConfig.setMaxIdle(redisConfig.getMaxIdle());
        jedisPoolConfig.setMinIdle(redisConfig.getMinIdle());
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        return jedisPoolConfig;
    }
}
