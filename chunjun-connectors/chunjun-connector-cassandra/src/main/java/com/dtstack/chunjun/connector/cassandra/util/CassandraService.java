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

package com.dtstack.chunjun.connector.cassandra.util;

import com.dtstack.chunjun.connector.cassandra.config.CassandraCommonConfig;
import com.dtstack.chunjun.connector.cassandra.config.CassandraSourceConfig;
import com.dtstack.chunjun.connector.cassandra.source.CassandraInputSplit;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.core.io.InputSplit;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Locale;

@Slf4j
public class CassandraService {

    private static final String TOKEN = "token(";

    private static final String RANDOM_PARTITIONER = "RandomPartitioner";

    private static final String MURMUR3_PARTITIONER = "Murmur3Partitioner";

    public static Session session(CassandraCommonConfig commonConfig) {
        String keySpace = commonConfig.getKeyspaces();

        Preconditions.checkNotNull(keySpace, "keySpace must not null");
        // 获取集群
        try (Cluster cluster = cluster(commonConfig)) {
            // 创建session
            Session cassandraSession = cluster.connect(keySpace);

            log.info("Get cassandra session successful");
            return cassandraSession;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取Cluster
     *
     * @param commonConfig cassandra配置
     * @return 返回Cluster实例
     */
    public static Cluster cluster(CassandraCommonConfig commonConfig) {
        try {
            Integer port = commonConfig.getPort();
            String hosts = commonConfig.getHost();

            String username = commonConfig.getUserName();
            String password = commonConfig.getPassword();

            String clusterName = commonConfig.getClusterName();

            HostDistance hostDistance =
                    hostDistance(
                            commonConfig.getHostDistance() == null
                                    ? "LOCAL"
                                    : commonConfig.getHostDistance());

            boolean useSSL = commonConfig.isUseSSL();
            int connectionsPerHost = commonConfig.getCoreConnectionsPerHost();
            int maxRequestsPerConnection = commonConfig.getMaxRequestsPerConnection();

            Integer readTimeoutMillis = commonConfig.getReadTimeoutMillis();
            Integer connectTimeoutMillis = commonConfig.getConnectTimeoutMillis();

            Preconditions.checkNotNull(hosts, "url must not null");

            // create cassandra cluster.
            Cluster.Builder builder =
                    Cluster.builder().addContactPoints(hosts.split(",")).withPort(port);

            builder =
                    StringUtils.isNotEmpty(clusterName)
                            ? builder.withClusterName(clusterName)
                            : builder;
            builder = useSSL ? builder.withSSL() : builder;

            if ((username != null) && !username.isEmpty()) {
                builder = builder.withCredentials(username, password);
            }

            SocketOptions socketOptions =
                    new SocketOptions()
                            .setReadTimeoutMillis(readTimeoutMillis)
                            .setConnectTimeoutMillis(connectTimeoutMillis);

            PoolingOptions poolingOptions =
                    hostDistance.equals(HostDistance.IGNORED)
                            ? new PoolingOptions()
                            : new PoolingOptions()
                                    .setConnectionsPerHost(
                                            hostDistance, connectionsPerHost, connectionsPerHost)
                                    .setMaxRequestsPerConnection(
                                            hostDistance, maxRequestsPerConnection)
                                    .setNewConnectionThreshold(hostDistance, 100);

            Cluster cassandraCluster =
                    builder.withPoolingOptions(poolingOptions)
                            .withSocketOptions(socketOptions)
                            .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                            .withReconnectionPolicy(
                                    new ExponentialReconnectionPolicy(3 * 1000, 60 * 1000))
                            .build();

            log.info("Get cassandra cluster successful");
            return cassandraCluster;
        } catch (Exception e) {
            throw new ChunJunRuntimeException(e);
        }
    }

    /**
     * According to the options, return {@link HostDistance}. Default {@link HostDistance#LOCAL}
     *
     * @param hostDistance hostDistance options.
     * @return {@link HostDistance}
     */
    public static HostDistance hostDistance(String hostDistance) {
        switch (hostDistance.toLowerCase(Locale.ROOT)) {
            case "remote":
                return HostDistance.REMOTE;
            case "ignored":
                return HostDistance.IGNORED;
            case "local":
            default:
                return HostDistance.LOCAL;
        }
    }

    public static ConsistencyLevel consistencyLevel(String consistency) {
        if (StringUtils.isEmpty(consistency)) {
            return ConsistencyLevel.LOCAL_QUORUM;
        } else {
            return ConsistencyLevel.valueOf(consistency);
        }
    }

    /**
     * Close cassandra cluster and session
     *
     * @param session cassandra session
     */
    public static void close(Session session) {
        if (session != null) {
            Cluster cluster = session.getCluster();

            if (cluster != null) {
                cluster.close();
                log.info("Close cassandra cluster successfully");
            }

            session.close();
            log.info("Close cassandra session successfully");
        }
    }

    public static InputSplit[] splitJob(
            CassandraSourceConfig sourceConf,
            int minNumSplits,
            ArrayList<CassandraInputSplit> splits) {

        String where = sourceConf.getWhere();

        if (minNumSplits <= 1) {
            splits.add(new CassandraInputSplit());
            return splits.toArray(new CassandraInputSplit[0]);
        }

        if (where != null && where.toLowerCase().contains(TOKEN)) {
            splits.add(new CassandraInputSplit());
            return splits.toArray(new CassandraInputSplit[0]);
        }
        Session session = CassandraService.session(sourceConf);
        String partitioner = session.getCluster().getMetadata().getPartitioner();
        if (partitioner.endsWith(RANDOM_PARTITIONER)) {
            BigDecimal minToken = BigDecimal.valueOf(-1);
            BigDecimal maxToken = new BigDecimal(new BigInteger("2").pow(127));
            BigDecimal step =
                    maxToken.subtract(minToken)
                            .divide(BigDecimal.valueOf(minNumSplits), 2, RoundingMode.HALF_EVEN);
            for (int i = 0; i < minNumSplits; i++) {
                BigInteger l = minToken.add(step.multiply(BigDecimal.valueOf(i))).toBigInteger();
                BigInteger r =
                        minToken.add(step.multiply(BigDecimal.valueOf(i + 1L))).toBigInteger();
                if (i == minNumSplits - 1) {
                    r = maxToken.toBigInteger();
                }
                splits.add(new CassandraInputSplit(l.toString(), r.toString()));
            }
        } else if (partitioner.endsWith(MURMUR3_PARTITIONER)) {
            BigDecimal minToken = BigDecimal.valueOf(Long.MIN_VALUE);
            BigDecimal maxToken = BigDecimal.valueOf(Long.MAX_VALUE);
            BigDecimal step =
                    maxToken.subtract(minToken)
                            .divide(BigDecimal.valueOf(minNumSplits), 2, RoundingMode.HALF_EVEN);
            for (int i = 0; i < minNumSplits; i++) {
                long l = minToken.add(step.multiply(BigDecimal.valueOf(i))).longValue();
                long r = minToken.add(step.multiply(BigDecimal.valueOf(i + 1L))).longValue();
                if (i == minNumSplits - 1) {
                    r = maxToken.longValue();
                }
                splits.add(new CassandraInputSplit(String.valueOf(l), String.valueOf(r)));
            }
        } else {
            splits.add(new CassandraInputSplit());
        }
        return splits.toArray(new CassandraInputSplit[0]);
    }

    /**
     * quote cassandra column
     *
     * @param column column name
     * @return column name like "column-name"
     */
    public static String quoteColumn(String column) {
        return "\"" + column + "\"";
    }
}
