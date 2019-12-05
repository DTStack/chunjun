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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Preconditions;;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.core.io.InputSplit;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;
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
            Log.info("Get cassandra session successful");
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

            Preconditions.checkNotNull(hosts, "url must not null");

            // 创建集群
            Cluster.Builder builder = Cluster.builder().addContactPoints(hosts.split(",")).withPort(port);
            builder = StringUtils.isNotEmpty(clusterName) ? builder.withClusterName(clusterName) : builder;
            builder = useSSL ? builder.withSSL() : builder;
            if ((username != null) && !username.isEmpty()) {
                builder = builder.withCredentials(username, password);
            }
            cassandraCluster = builder.build();

            Log.info("Get cassandra cluster successful");
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
                value = row.getBytes(columnName);
            } else if (type == DataType.timestamp()) {
                value = row.getDate(columnName);
            } else if (type == DataType.decimal()) {
                value = row.getDecimal(columnName);
            } else if (type == DataType.cfloat()) {
                value = row.getFloat(columnName);
            } else if (type == DataType.inet()) {
                value = row.getInet(columnName);
            } else if (type == DataType.cint()) {
                value = row.getInt(columnName);
            } else if (type == DataType.varchar()) {
                value = row.getString(columnName);
            } else if (type == DataType.uuid() || type == DataType.timeuuid()) {
                value = row.getUUID(columnName);
            } else if (type == DataType.varint()) {
                value = row.getVarint(columnName);
            } else if (type == DataType.cdouble()) {
                value = row.getDouble(columnName);
            } else if (type == DataType.text()) {
                value = row.getString(columnName);
            }

        } catch (Exception e) {
            Log.info("获取'{}'值发生异常：{}", columnName, e);
        }

        if (value == null) {
            Log.info("Column '{}' Type({}) get cassandra data is NULL.", columnName, type);
        }
        return value;
    }

    /**
     * javaClass转cql类型
     * @param clazz javaClass
     * @return cql类型
     */
    private static DataType valueOf(Class<?> clazz) {

        if (clazz == null) {
            return DataType.custom("NULL");
        }
        if (clazz == long.class || clazz == Long.class) {
            return DataType.bigint();
        }
        if (clazz == boolean.class || clazz == Boolean.class) {
            return DataType.cboolean();
        }
        if (clazz == Byte.class || clazz == byte.class) {
            return DataType.blob();
        }
        if (clazz == Date.class || clazz == Timestamp.class) {
            return DataType.timestamp();
        }
        if (clazz == BigDecimal.class) {
            return DataType.decimal();
        }
        if (clazz == float.class || clazz == Float.class) {
            return DataType.cfloat();
        }
        if (clazz == InetAddress.class) {
            return DataType.inet();
        }
        if (clazz == int.class || clazz == Integer.class) {
            return DataType.cint();
        }
        if (clazz == String.class) {
            return DataType.varchar();
        }
        if (clazz == UUID.class) {
            return DataType.uuid();
        }
        if (clazz == BigInteger.class) {
            return DataType.varint();
        }
        LOG.info("Class '{}' unknow DataType in cassandra.", clazz);

        return DataType.custom("unknow");
    }
}
