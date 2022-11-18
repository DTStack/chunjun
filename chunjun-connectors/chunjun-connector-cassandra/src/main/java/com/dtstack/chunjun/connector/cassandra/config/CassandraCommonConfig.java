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

package com.dtstack.chunjun.connector.cassandra.config;

import com.dtstack.chunjun.config.CommonConfig;

import org.apache.flink.configuration.ReadableConfig;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.CLUSTER_NAME;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.CONNECT_TIMEOUT_MILLISECONDS;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.CONSISTENCY;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.CORE_CONNECTIONS_PER_HOST;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.HOST;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.HOST_DISTANCE;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.KEY_SPACES;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.MAX_CONNECTIONS__PER_HOST;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.MAX_QUEUE_SIZE;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.MAX_REQUESTS_PER_CONNECTION;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.PASSWORD;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.POOL_TIMEOUT_MILLISECONDS;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.PORT;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.READ_TIME_OUT_MILLISECONDS;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.TABLE_NAME;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.USER_NAME;
import static com.dtstack.chunjun.connector.cassandra.optinos.CassandraCommonOptions.USE_SSL;

public class CassandraCommonConfig extends CommonConfig {

    protected String host;

    protected Integer port;

    protected String userName;

    protected String password;

    protected String tableName;

    protected String keyspaces;

    protected String hostDistance = "LOCAL";

    protected boolean useSSL = false;

    protected String clusterName = "chunjun-cluster";

    protected String consistency = "LOCAL_QUORUM";

    protected Integer coreConnectionsPerHost = 8;

    protected Integer maxConnectionsPerHost = 32768;

    protected Integer maxRequestsPerConnection = 1;

    protected Integer maxQueueSize = 10 * 1000;

    protected Integer readTimeoutMillis = 60 * 1000;

    protected Integer connectTimeoutMillis = 60 * 1000;

    protected Integer poolTimeoutMillis = 60 * 1000;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getKeyspaces() {
        return keyspaces;
    }

    public void setKeyspaces(String keyspaces) {
        this.keyspaces = keyspaces;
    }

    public String getHostDistance() {
        return hostDistance;
    }

    public void setHostDistance(String hostDistance) {
        this.hostDistance = hostDistance;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getConsistency() {
        return consistency;
    }

    public void setConsistency(String consistency) {
        this.consistency = consistency;
    }

    public Integer getMaxRequestsPerConnection() {
        return maxRequestsPerConnection;
    }

    public void setMaxRequestsPerConnection(Integer maxRequestsPerConnection) {
        this.maxRequestsPerConnection = maxRequestsPerConnection;
    }

    public Integer getCoreConnectionsPerHost() {
        return coreConnectionsPerHost;
    }

    public void setCoreConnectionsPerHost(Integer coreConnectionsPerHost) {
        this.coreConnectionsPerHost = coreConnectionsPerHost;
    }

    public Integer getMaxConnectionsPerHost() {
        return maxConnectionsPerHost;
    }

    public void setMaxConnectionsPerHost(Integer maxConnectionsPerHost) {
        this.maxConnectionsPerHost = maxConnectionsPerHost;
    }

    public Integer getMaxQueueSize() {
        return maxQueueSize;
    }

    public void setMaxQueueSize(Integer maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    public Integer getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    public void setReadTimeoutMillis(Integer readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
    }

    public Integer getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(Integer connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public Integer getPoolTimeoutMillis() {
        return poolTimeoutMillis;
    }

    public void setPoolTimeoutMillis(Integer poolTimeoutMillis) {
        this.poolTimeoutMillis = poolTimeoutMillis;
    }

    public static CassandraCommonConfig from(
            ReadableConfig readableConfig, CassandraCommonConfig conf) {
        conf.setHost(readableConfig.get(HOST));
        conf.setPort(readableConfig.get(PORT));
        conf.setUserName(readableConfig.get(USER_NAME));
        conf.setPassword(readableConfig.get(PASSWORD));
        conf.setUseSSL(readableConfig.get(USE_SSL));

        conf.setTableName(readableConfig.get(TABLE_NAME));
        conf.setKeyspaces(readableConfig.get(KEY_SPACES));

        conf.setHostDistance(readableConfig.get(HOST_DISTANCE));
        conf.setClusterName(readableConfig.get(CLUSTER_NAME));
        conf.setConsistency(readableConfig.get(CONSISTENCY));

        conf.setCoreConnectionsPerHost(readableConfig.get(CORE_CONNECTIONS_PER_HOST));
        conf.setMaxConnectionsPerHost(readableConfig.get(MAX_CONNECTIONS__PER_HOST));
        conf.setMaxQueueSize(readableConfig.get(MAX_QUEUE_SIZE));
        conf.setMaxRequestsPerConnection(readableConfig.get(MAX_REQUESTS_PER_CONNECTION));

        conf.setReadTimeoutMillis(readableConfig.get(READ_TIME_OUT_MILLISECONDS));
        conf.setConnectTimeoutMillis(readableConfig.get(CONNECT_TIMEOUT_MILLISECONDS));
        conf.setPoolTimeoutMillis(readableConfig.get(POOL_TIMEOUT_MILLISECONDS));

        return conf;
    }

    @Override
    public String toString() {
        ToStringBuilder toStringBuilder =
                new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                        .append("host", host)
                        .append("port", port)
                        .append("userName", userName)
                        .append("tableName", tableName)
                        .append("keyspaces", keyspaces)
                        .append("hostDistance", hostDistance)
                        .append("useSSL", useSSL)
                        .append("clusterName", clusterName)
                        .append("consistency", consistency)
                        .append("coreConnectionsPerHost", coreConnectionsPerHost)
                        .append("maxConnectionsPerHost", maxConnectionsPerHost)
                        .append("maxRequestPerConnection", maxRequestsPerConnection)
                        .append("readTimeoutMillis", readTimeoutMillis)
                        .append("connectTimeoutMillis", connectTimeoutMillis)
                        .append("poolTimeoutMillis", poolTimeoutMillis);

        return toStringBuilder.toString();
    }
}
