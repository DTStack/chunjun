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

package com.dtstack.flinkx.connector.cassandra.util;

import org.apache.flink.core.io.InputSplit;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.dtstack.flinkx.connector.cassandra.CassandraConstants;
import com.dtstack.flinkx.connector.cassandra.conf.CassandraCommonConf;
import com.dtstack.flinkx.connector.cassandra.conf.CassandraSourceConf;
import com.dtstack.flinkx.connector.cassandra.source.CassandraInputSplit;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.ExceptionUtil;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraService {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraService.class);

    /**
     * Build cassandra session.
     *
     * @param commonConf cassandra common conf {@link CassandraCommonConf}
     * @return cassandraSession
     */
    public static Session session(CassandraCommonConf commonConf) {
        Session cassandraSession;
        try {
            String keySpace = commonConf.getKeyspaces();

            Preconditions.checkNotNull(keySpace, "keySpace must not null");

            // 获取集群
            Cluster cluster = cluster(commonConf);

            // 创建session
            cassandraSession = cluster.connect(keySpace);

            LOG.info("Get cassandra session successful");
            return cassandraSession;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取Cluster
     *
     * @param commonConf cassandra配置
     * @return 返回Cluster实例
     */
    public static Cluster cluster(CassandraCommonConf commonConf) {
        try {
            Integer port = commonConf.getPort();
            String hosts = commonConf.getHost();

            String username = commonConf.getUserName();
            String password = commonConf.getPassword();

            String clusterName = commonConf.getClusterName();

            HostDistance hostDistance = hostDistance(commonConf.getHostDistance());

            boolean useSSL = commonConf.isUseSSL();
            int connectionsPerHost = commonConf.getCoreConnectionsPerHost();
            int maxRequestsPerConnection = commonConf.getMaxRequestsPerConnection();

            Integer readTimeoutMillis = commonConf.getReadTimeoutMillis();
            Integer connectTimeoutMillis = commonConf.getConnectTimeoutMillis();

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

            LOG.info("Get cassandra cluster successful");
            return cassandraCluster;
        } catch (Exception e) {
            throw new FlinkxRuntimeException(e);
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
                LOG.info("Close cassandra cluster successfully");
            }

            session.close();
            LOG.info("Close cassandra session successfully");
        }
    }

    /**
     * 从cassandra中获取数据
     *
     * @param row 一行数据
     * @param type 字段类型
     * @param columnName 字段名
     * @return 返回该字段对应的值
     */
    public static Object getData(Row row, DataType type, String columnName) {
        Object value = null;

        try {
            if (type == DataType.bigint()) {
                value = row.getLong(columnName);
            } else if (type == DataType.cboolean()) {
                value = row.getBool(columnName);
            } else if (type == DataType.blob()) {
                value = row.getBytes(columnName).array();
            } else if (type == DataType.timestamp()) {
                value = row.getTimestamp(columnName);
            } else if (type == DataType.decimal()) {
                value = row.getDecimal(columnName);
            } else if (type == DataType.cfloat()) {
                value = row.getFloat(columnName);
            } else if (type == DataType.inet()) {
                value = row.getInet(columnName).getHostAddress();
            } else if (type == DataType.cint()) {
                value = row.getInt(columnName);
            } else if (type == DataType.varchar()) {
                value = row.getString(columnName);
            } else if (type == DataType.uuid() || type == DataType.timeuuid()) {
                value = row.getUUID(columnName).toString();
            } else if (type == DataType.varint()) {
                value = row.getVarint(columnName);
            } else if (type == DataType.cdouble()) {
                value = row.getDouble(columnName);
            } else if (type == DataType.text()) {
                value = row.getString(columnName);
            } else if (type == DataType.ascii()) {
                value = row.getString(columnName);
            } else if (type == DataType.smallint()) {
                value = row.getShort(columnName);
            } else if (type == DataType.tinyint()) {
                value = row.getByte(columnName);
            } else if (type == DataType.date()) {
                value = row.getDate(columnName).getMillisSinceEpoch();
            } else if (type == DataType.time()) {
                value = row.getTime(columnName);
            }
        } catch (Exception e) {
            LOG.info("获取'{}'值发生异常：{}", columnName, e);
        }

        if (value == null) {
            LOG.info("Column '{}' Type({}) get cassandra data is NULL.", columnName, type);
        }
        return value;
    }

    /**
     * 对象转byte[]
     *
     * @param obj 对象实例
     * @param <T> 对象类型
     * @return Optional<byte[]>
     */
    private static <T> Optional<byte[]> objectToBytes(T obj) {
        byte[] bytes = null;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream sOut;
        try {
            sOut = new ObjectOutputStream(out);
            sOut.writeObject(obj);
            sOut.flush();
            bytes = out.toByteArray();
        } catch (IOException e) {
            LOG.warn(
                    "object convent byte[] failed, error info {}",
                    ExceptionUtil.getErrorMessage(e),
                    e);
        }
        return Optional.ofNullable(bytes);
    }

    /**
     * 设置值到对应的pos上
     *
     * @param ps preStatement
     * @param pos 位置
     * @param sqlType cql类型
     * @param value 值
     * @throws Exception 对于不支持的数据类型，抛出异常
     */
    public static void bindColumn(BoundStatement ps, int pos, DataType sqlType, Object value)
            throws Exception {
        if (value != null) {
            switch (sqlType.getName()) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    ps.setString(pos, (String) value);
                    break;

                case BLOB:
                    ps.setBytes(
                            pos,
                            ByteBuffer.wrap(objectToBytes(value).orElseGet(() -> new byte[8])));
                    break;

                case BOOLEAN:
                    ps.setBool(pos, (Boolean) value);
                    break;

                case TINYINT:
                    ps.setByte(pos, ((Integer) value).byteValue());
                    break;

                case SMALLINT:
                    ps.setShort(pos, ((Integer) value).shortValue());
                    break;

                case INT:
                    ps.setInt(pos, (Integer) value);
                    break;

                case BIGINT:
                    ps.setLong(pos, (Long) value);
                    break;

                case VARINT:
                    ps.setVarint(pos, BigInteger.valueOf((Long) value));
                    break;

                case FLOAT:
                    ps.setFloat(pos, (Float) value);
                    break;

                case DOUBLE:
                    ps.setDouble(pos, (Double) value);
                    break;

                case DECIMAL:
                    ps.setDecimal(pos, (BigDecimal) value);
                    break;

                case DATE:
                    ps.setDate(pos, LocalDate.fromMillisSinceEpoch(((Date) value).getTime()));
                    break;

                case TIME:
                    ps.setTime(pos, ((Time) value).getTime());
                    break;

                case TIMESTAMP:
                    ps.setTimestamp(pos, (Date) value);
                    break;

                case UUID:
                case TIMEUUID:
                    ps.setUUID(pos, UUID.fromString((String) value));
                    break;

                case INET:
                    ps.setInet(pos, InetAddress.getByName((String) value));
                    break;

                default:
                    throw new RuntimeException("暂不支持该数据类型: " + sqlType);
            }
        } else {
            ps.setToNull(pos);
        }
    }

    /**
     * 分割任务
     *
     * @param minNumSplits 分片数
     * @param splits 分片列表
     * @return 返回InputSplit[]
     */
    public static InputSplit[] splitJob(
            CassandraSourceConf sourceConf,
            int minNumSplits,
            ArrayList<CassandraInputSplit> splits) {

        String where = sourceConf.getWhere();

        if (minNumSplits <= 1) {
            splits.add(new CassandraInputSplit());
            return splits.toArray(new CassandraInputSplit[0]);
        }

        if (where != null && where.toLowerCase().contains(CassandraConstants.TOKEN)) {
            splits.add(new CassandraInputSplit());
            return splits.toArray(new CassandraInputSplit[0]);
        }
        Session session = CassandraService.session(sourceConf);
        String partitioner = session.getCluster().getMetadata().getPartitioner();
        if (partitioner.endsWith(CassandraConstants.RANDOM_PARTITIONER)) {
            BigDecimal minToken = BigDecimal.valueOf(-1);
            BigDecimal maxToken = new BigDecimal(new BigInteger("2").pow(127));
            BigDecimal step =
                    maxToken.subtract(minToken)
                            .divide(
                                    BigDecimal.valueOf(minNumSplits),
                                    2,
                                    BigDecimal.ROUND_HALF_EVEN);
            for (int i = 0; i < minNumSplits; i++) {
                BigInteger l = minToken.add(step.multiply(BigDecimal.valueOf(i))).toBigInteger();
                BigInteger r =
                        minToken.add(step.multiply(BigDecimal.valueOf(i + 1L))).toBigInteger();
                if (i == minNumSplits - 1) {
                    r = maxToken.toBigInteger();
                }
                splits.add(new CassandraInputSplit(l.toString(), r.toString()));
            }
        } else if (partitioner.endsWith(CassandraConstants.MURMUR3_PARTITIONER)) {
            BigDecimal minToken = BigDecimal.valueOf(Long.MIN_VALUE);
            BigDecimal maxToken = BigDecimal.valueOf(Long.MAX_VALUE);
            BigDecimal step =
                    maxToken.subtract(minToken)
                            .divide(
                                    BigDecimal.valueOf(minNumSplits),
                                    2,
                                    BigDecimal.ROUND_HALF_EVEN);
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
}
