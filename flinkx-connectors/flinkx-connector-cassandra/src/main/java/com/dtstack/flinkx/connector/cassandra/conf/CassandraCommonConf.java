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

package com.dtstack.flinkx.connector.cassandra.conf;

import com.dtstack.flinkx.conf.FlinkxCommonConf;

/**
 * @author tiezhu
 * @since 2021/6/21 星期一
 */
public class CassandraCommonConf extends FlinkxCommonConf {

    protected String host;

    protected Integer port;

    protected String userName;

    protected String password;

    protected String tableName;

    protected String keyspaces;

    protected String hostDistance;

    protected boolean useSSL;

    private String clusterName;

    private String consistency;

    private Integer coreConnectionsPerHost;

    private Integer maxConnectionsPerHost;

    private Integer maxRequestsPerConnection;

    private Integer maxQueueSize;

    private Integer readTimeoutMillis;

    private Integer connectTimeoutMillis;

    private Integer poolTimeoutMillis;

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
}
