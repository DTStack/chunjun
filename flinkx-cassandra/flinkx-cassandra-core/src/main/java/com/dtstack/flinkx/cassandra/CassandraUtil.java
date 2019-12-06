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

package com.dtstack.flinkx.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.LocalDate;
import com.google.common.base.Preconditions;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static com.dtstack.flinkx.cassandra.CassandraConfigKeys.*;

/**
 *
 * @Company: www.dtstack.com
 * @author wuhui
 */
public class CassandraUtil {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraUtil.class);

    /**
     * 获取cassandraSession
     * @param cassandraConfig cassandra配置
     * @param clusterName 集群名称，可以空字符串，用于重新连接使用
     * @return cassandraSession
     */
    public static Session getSession(Map<String,Object> cassandraConfig, String clusterName) {
        Session cassandraSession;
        try {
            String keySpace = MapUtils.getString(cassandraConfig, KEY_KEY_SPACE);

            Preconditions.checkNotNull(keySpace, "keySpace must not null");

            // 获取集群
            Cluster cluster = getCluster(cassandraConfig, clusterName);

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
     * @param cassandraConfig cassandra配置
     * @param clusterName 集群名称
     * @return 返回Cluster实例
     */
    private static Cluster getCluster(Map<String,Object> cassandraConfig, String clusterName) {
        Cluster cassandraCluster;
        try {
            String username = MapUtils.getString(cassandraConfig, KEY_USERNAME);
            String password = MapUtils.getString(cassandraConfig, KEY_PASSWORD);
            Integer port = MapUtils.getInteger(cassandraConfig, KEY_PORT);
            String hosts = MapUtils.getString(cassandraConfig, KEY_HOST);
            boolean useSSL = MapUtils.getBooleanValue(cassandraConfig, KEY_USE_SSL);
            int connectionsPerHost = MapUtils.getIntValue(cassandraConfig, KEY_CONNECTION_PER_HOST, 8);
            int maxPendingPerConnection = MapUtils.getIntValue(cassandraConfig, KEY_MAX_PENDING_CONNECTION, 128);

            Preconditions.checkNotNull(hosts, "url must not null");

            // 创建集群
            Cluster.Builder builder = Cluster.builder().addContactPoints(hosts.split(",")).withPort(port);
            builder = StringUtils.isNotEmpty(clusterName) ? builder.withClusterName(clusterName) : builder;
            builder = useSSL ? builder.withSSL() : builder;
            if ((username != null) && !username.isEmpty()) {
                builder = builder.withCredentials(username, password);
            }

            PoolingOptions poolingOptions = new PoolingOptions()
                    .setConnectionsPerHost(HostDistance.LOCAL, connectionsPerHost, connectionsPerHost)
                    .setMaxRequestsPerConnection(HostDistance.LOCAL, maxPendingPerConnection)
                    .setNewConnectionThreshold(HostDistance.LOCAL, 100);

            cassandraCluster = builder.withPoolingOptions(poolingOptions).build();

            LOG.info("Get cassandra cluster successful");
            return cassandraCluster;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *  关闭集群与会话
     * @param session 会话实例
     */
    public static void close(Session session){
        Cluster cluster = null;
        if (session != null){
            LOG.info("Start close cassandra session");
            cluster = session.getCluster();
            session.close();
            LOG.info("Close cassandra session successfully");
        }

        if (cluster != null){
            LOG.info("Start close cassandra cluster");
            cluster.close();
            LOG.info("Close cassandra cluster successfully");
        }
    }

    /**
     * 从cassandra中获取数据
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
     * @param obj 对象实例
     * @param <T> 对象类型
     * @return Optional<byte[]>
     */
    private static<T> Optional<byte[]> objectToBytes(T obj){
        byte[] bytes = null;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream sOut;
        try {
            sOut = new ObjectOutputStream(out);
            sOut.writeObject(obj);
            sOut.flush();
            bytes= out.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Optional.ofNullable(bytes);
    }

    /**
     * 设置值到对应的pos上
     * @param ps preStatement
     * @param pos 位置
     * @param sqlType cql类型
     * @param value 值
     * @throws RuntimeException 对于不支持的数据类型，抛出异常
     */
    public static void bindColumn(BoundStatement ps, int pos, DataType sqlType, Object value) throws Exception {
        if (value != null) {
            switch (sqlType.getName()) {
                case ASCII:
                case TEXT:
                case VARCHAR:
                    ps.setString(pos, (String) value);
                    break;

                case BLOB:
                    ps.setBytes(pos, ByteBuffer.wrap(objectToBytes(value).orElseGet(() -> new byte[8])));
                    break;

                case BOOLEAN:
                    ps.setBool(pos, (Boolean) value);
                    break;

                case TINYINT:
                    ps.setByte(pos, ((Integer)value).byteValue());
                    break;

                case SMALLINT:
                    ps.setShort(pos, ((Integer)value).shortValue());
                    break;

                case INT:
                    ps.setInt(pos, (Integer)value);
                    break;

                case BIGINT:
                    ps.setLong(pos, (Long)value);
                    break;

                case VARINT:
                    ps.setVarint(pos, BigInteger.valueOf((Long) value));
                    break;

                case FLOAT:
                    ps.setFloat(pos, (Float)value);
                    break;

                case DOUBLE:
                    ps.setDouble(pos, (Double) value);
                    break;

                case DECIMAL:
                    ps.setDecimal(pos, (BigDecimal) value);
                    break;

                case DATE:
                    ps.setDate(pos, LocalDate.fromMillisSinceEpoch(((Date)value).getTime()));
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
}
