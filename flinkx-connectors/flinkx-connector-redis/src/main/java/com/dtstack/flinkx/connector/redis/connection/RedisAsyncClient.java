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

package com.dtstack.flinkx.connector.redis.connection;

import com.dtstack.flinkx.connector.redis.conf.RedisConf;
import com.dtstack.flinkx.util.ExceptionUtil;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.internal.HostAndPort;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;

import static com.dtstack.flinkx.connector.redis.options.RedisOptions.REDIS_HOST_PATTERN;

/**
 * @author chuixue
 * @create 2021-06-22 15:08
 * @description
 */
public class RedisAsyncClient {

    private static final Logger LOG = LoggerFactory.getLogger(RedisAsyncClient.class);

    private RedisClient redisClient;

    private StatefulRedisConnection<String, String> connection;

    private RedisClusterClient clusterClient;

    private StatefulRedisClusterConnection<String, String> clusterConnection;

    private final RedisConf redisConf;

    public RedisAsyncClient(RedisConf redisConf) {
        this.redisConf = redisConf;
    }

    public RedisKeyAsyncCommands<String, String> getRedisKeyAsyncCommands() {
        RedisKeyAsyncCommands<String, String> redisKeyAsyncCommands = null;
        for (int i = 0; i <= 2; i++) {
            try {
                LOG.info("connect " + (i + 1) + " times.");
                redisKeyAsyncCommands = getRedisKeyAsyncCommandsInner();
                if (redisKeyAsyncCommands != null) {
                    LOG.info("jedis is connected = {} ", redisKeyAsyncCommands);
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
        return redisKeyAsyncCommands;
    }

    private RedisKeyAsyncCommands<String, String> getRedisKeyAsyncCommandsInner() {
        String url = redisConf.getHostPort();
        String password = redisConf.getPassword();
        int database = redisConf.getDatabase();

        switch (redisConf.getRedisConnectType()) {
            case STANDALONE:
                RedisURI redisURI = RedisURI.create("redis://" + url);
                if (!Objects.isNull(password)) {
                    redisURI.setPassword(password);
                }
                redisURI.setDatabase(database);
                redisClient = RedisClient.create(redisURI);
                connection = redisClient.connect();
                return connection.async();
            case SENTINEL:
                String[] urlSplit = StringUtils.split(url, ",");
                RedisURI.Builder builder = null;
                for (String item : urlSplit) {
                    Matcher mather = REDIS_HOST_PATTERN.defaultValue().matcher(item);
                    if (mather.find()) {
                        builder =
                                buildSentinelUri(
                                        mather.group("host"), mather.group("port"), builder);
                    } else {
                        throw new IllegalArgumentException(
                                String.format("Illegal format with redis url [%s]", item));
                    }
                }

                if (Objects.nonNull(builder)) {
                    builder.withPassword(redisConf.getPassword())
                            .withDatabase(redisConf.getDatabase())
                            .withSentinelMasterId(redisConf.getMasterName());
                } else {
                    throw new NullPointerException("build redis uri error!");
                }

                RedisURI uri = builder.build();
                redisClient = RedisClient.create(uri);
                connection = redisClient.connect();
                return connection.async();
            case CLUSTER:
                List<RedisURI> clusterURIs = buildClusterURIs(url);
                clusterClient = RedisClusterClient.create(clusterURIs);
                clusterConnection = clusterClient.connect();
                return clusterConnection.async();
            default:
                throw new IllegalArgumentException(
                        "unsupported redis type[ " + redisConf.getType().getType() + "]");
        }
    }

    public void close() {
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
        if (clusterConnection != null) {
            clusterConnection.close();
        }
        if (clusterClient != null) {
            clusterClient.shutdown();
        }
    }

    private List<RedisURI> buildClusterURIs(String url) {
        String password = redisConf.getPassword();
        int database = redisConf.getDatabase();
        String[] addresses = StringUtils.split(url, ",");
        List<RedisURI> redisURIs = new ArrayList<>(addresses.length);
        for (String addr : addresses) {
            HostAndPort hostAndPort = HostAndPort.parse(addr);
            RedisURI redisURI = RedisURI.create(hostAndPort.hostText, hostAndPort.port);
            if (StringUtils.isNotEmpty(password)) {
                redisURI.setPassword(password);
            }
            redisURI.setDatabase(database);
            redisURIs.add(redisURI);
        }
        return redisURIs;
    }

    private RedisURI.Builder buildSentinelUri(String host, String port, RedisURI.Builder builder) {
        if (Objects.nonNull(builder)) {
            builder.withSentinel(host, Integer.parseInt(port));
        } else {
            builder = RedisURI.Builder.sentinel(host, Integer.parseInt(port));
        }
        return builder;
    }
}
