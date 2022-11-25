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

import com.dtstack.chunjun.connector.redis.conf.RedisConf;
import com.dtstack.chunjun.util.ExceptionUtil;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static com.dtstack.chunjun.connector.redis.options.RedisOptions.REDIS_DEFAULT_PORT;
import static com.dtstack.chunjun.connector.redis.options.RedisOptions.REDIS_HOST_PATTERN;

public class RedisSyncClient {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSyncClient.class);

    private JedisPool pool;

    private JedisCommands jedis;

    private JedisSentinelPool jedisSentinelPool;

    private final RedisConf redisConf;

    public RedisSyncClient(RedisConf redisConf) {
        this.redisConf = redisConf;
    }

    private JedisCommands getJedisInner() {
        JedisPoolConfig poolConfig = getConfig();
        String[] nodes = StringUtils.split(redisConf.getHostPort(), ",");

        switch (redisConf.getRedisConnectType()) {
            case STANDALONE:
                String firstIp = null;
                String firstPort = null;
                Matcher standalone = REDIS_HOST_PATTERN.defaultValue().matcher(nodes[0]);
                if (standalone.find()) {
                    firstIp = standalone.group("host").trim();
                    firstPort = standalone.group("port").trim();
                    firstPort = firstPort == null ? REDIS_DEFAULT_PORT.defaultValue() : firstPort;
                }
                if (Objects.nonNull(firstIp) && pool == null) {
                    pool =
                            new JedisPool(
                                    poolConfig,
                                    firstIp,
                                    Integer.parseInt(firstPort),
                                    redisConf.getTimeout(),
                                    redisConf.getPassword(),
                                    redisConf.getDatabase());
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
                                    redisConf.getMasterName(),
                                    ipPorts,
                                    poolConfig,
                                    redisConf.getTimeout(),
                                    redisConf.getPassword(),
                                    redisConf.getDatabase());
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
                                redisConf.getTimeout(),
                                redisConf.getTimeout(),
                                10,
                                redisConf.getPassword(),
                                poolConfig);
                break;
            default:
                throw new IllegalArgumentException(
                        "unsupported redis type[ " + redisConf.getType().getType() + "]");
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
                LOG.info("connect " + (i + 1) + " times.");
                jedisInner = getJedisInner();
                if (jedisInner != null) {
                    LOG.info("jedis is connected = {} ", jedisInner);
                    break;
                }
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                LOG.error(
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
            LOG.error("close jedis error", e);
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
            LOG.error(ExceptionUtil.getErrorMessage(e));
        }
    }

    private JedisPoolConfig getConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(redisConf.getMaxTotal());
        jedisPoolConfig.setMaxIdle(redisConf.getMaxIdle());
        jedisPoolConfig.setMinIdle(redisConf.getMinIdle());
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnReturn(true);
        return jedisPoolConfig;
    }
}
