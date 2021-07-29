/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.dtstack.flinkx.mongodb;

import com.mongodb.AuthenticationMechanism;

import java.io.Serializable;
import java.util.List;

/**
 * @author jiangbo
 * @date 2019/12/5
 */
public class MongodbConfig implements Serializable {

    private String hostPorts;

    private String url;

    private String username;

    private String password;

    private String authenticationMechanism = AuthenticationMechanism.SCRAM_SHA_1.getMechanismName();

    private String database;

    private String collectionName;

    private String filter;

    private int fetchSize;

    private String writeMode;

    private String replaceKey;

    private List<String> monitorDatabases;

    private List<String> monitorCollections;

    private List<String> operateType;

    private boolean pavingData;

    private String clusterMode;

    private int startLocation;

    private boolean excludeDocId;

    private ConnectionConfig mongodbConfig = new ConnectionConfig();

    public class ConnectionConfig implements Serializable{
        private int connectionsPerHost = 100;

        private int threadsForConnectionMultiplier = 100;

        private int connectionTimeout = 10000;

        private int maxWaitTime = 5000;

        private int socketTimeout = 0;

        public int getConnectionsPerHost() {
            return connectionsPerHost;
        }

        public void setConnectionsPerHost(int connectionsPerHost) {
            this.connectionsPerHost = connectionsPerHost;
        }

        public int getThreadsForConnectionMultiplier() {
            return threadsForConnectionMultiplier;
        }

        public void setThreadsForConnectionMultiplier(int threadsForConnectionMultiplier) {
            this.threadsForConnectionMultiplier = threadsForConnectionMultiplier;
        }

        public int getConnectionTimeout() {
            return connectionTimeout;
        }

        public void setConnectionTimeout(int connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
        }

        public int getMaxWaitTime() {
            return maxWaitTime;
        }

        public void setMaxWaitTime(int maxWaitTime) {
            this.maxWaitTime = maxWaitTime;
        }

        public int getSocketTimeout() {
            return socketTimeout;
        }

        public void setSocketTimeout(int socketTimeout) {
            this.socketTimeout = socketTimeout;
        }

        @Override
        public String toString() {
            return "ConnectionConfig{" +
                    "connectionsPerHost=" + connectionsPerHost +
                    ", threadsForConnectionMultiplier=" + threadsForConnectionMultiplier +
                    ", connectionTimeout=" + connectionTimeout +
                    ", maxWaitTime=" + maxWaitTime +
                    ", socketTimeout=" + socketTimeout +
                    '}';
        }
    }

    public boolean getExcludeDocId() {
        return excludeDocId;
    }

    public void setExcludeDocId(boolean excludeDocId) {
        this.excludeDocId = excludeDocId;
    }

    public int getStartLocation() {
        return startLocation;
    }

    public void setStartLocation(int startLocation) {
        this.startLocation = startLocation;
    }

    public String getClusterMode() {
        return clusterMode;
    }

    public void setClusterMode(String clusterMode) {
        this.clusterMode = clusterMode;
    }

    public List<String> getMonitorDatabases() {
        return monitorDatabases;
    }

    public void setMonitorDatabases(List<String> monitorDatabases) {
        this.monitorDatabases = monitorDatabases;
    }

    public List<String> getMonitorCollections() {
        return monitorCollections;
    }

    public void setMonitorCollections(List<String> monitorCollections) {
        this.monitorCollections = monitorCollections;
    }

    public String getAuthenticationMechanism() {
        return authenticationMechanism;
    }

    public void setAuthenticationMechanism(String authenticationMechanism) {
        this.authenticationMechanism = authenticationMechanism;
    }

    public String getHostPorts() {
        return hostPorts;
    }

    public void setHostPorts(String hostPorts) {
        this.hostPorts = hostPorts;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public String getReplaceKey() {
        return replaceKey;
    }

    public void setReplaceKey(String replaceKey) {
        this.replaceKey = replaceKey;
    }

    public ConnectionConfig getMongodbConfig() {
        return mongodbConfig;
    }

    public void setMongodbConfig(ConnectionConfig mongodbConfig) {
        this.mongodbConfig = mongodbConfig;
    }

    public List<String> getOperateType() {
        return operateType;
    }

    public void setOperateType(List<String> operateType) {
        this.operateType = operateType;
    }

    public boolean getPavingData() {
        return pavingData;
    }

    public void setPavingData(boolean pavingData) {
        this.pavingData = pavingData;
    }

    @Override
    public String toString() {
        return "MongodbConfig{" +
                "hostPorts='" + hostPorts + '\'' +
                ", url='" + url + '\'' +
                ", username='" + username + '\'' +
                ", password='******" + '\'' +
                ", authenticationMechanism='" + authenticationMechanism + '\'' +
                ", database='" + database + '\'' +
                ", collectionName='" + collectionName + '\'' +
                ", filter='" + filter + '\'' +
                ", fetchSize=" + fetchSize +
                ", writeMode='" + writeMode + '\'' +
                ", replaceKey='" + replaceKey + '\'' +
                ", monitorDatabases=" + monitorDatabases +
                ", monitorCollections=" + monitorCollections +
                ", operateType=" + operateType +
                ", pavingData=" + pavingData +
                ", clusterMode='" + clusterMode + '\'' +
                ", startLocation=" + startLocation +
                ", excludeDocId=" + excludeDocId +
                ", mongodbConfig=" + mongodbConfig +
                '}';
    }
}
