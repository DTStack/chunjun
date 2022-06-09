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

package com.dtstack.chunjun.connector.mongodb.datasync;

import com.dtstack.chunjun.conf.ChunJunCommonConf;
import com.dtstack.chunjun.connector.mongodb.conf.MongodbClientOptions;

import com.mongodb.AuthenticationMechanism;

import java.io.Serializable;
import java.util.List;

/**
 * @author Ada Wong
 * @program chunjun
 * @create 2021/06/21
 */
public class MongodbDataSyncConf extends ChunJunCommonConf implements Serializable {

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

    private String clusterMode;

    private int startLocation;

    private boolean excludeDocId;

    private MongodbClientOptions mongodbConfig = new MongodbClientOptions();

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

    public MongodbClientOptions getMongodbConfig() {
        return mongodbConfig;
    }

    public void setMongodbConfig(MongodbClientOptions mongodbConfig) {
        this.mongodbConfig = mongodbConfig;
    }

    @Override
    public String toString() {
        return "MongodbConfig{"
                + "hostPorts='"
                + hostPorts
                + '\''
                + ", url='"
                + url
                + '\''
                + ", username='"
                + username
                + '\''
                + ", password='"
                + "******"
                + '\''
                + ", authenticationMechanism='"
                + authenticationMechanism
                + '\''
                + ", database='"
                + database
                + '\''
                + ", collectionName='"
                + collectionName
                + '\''
                + ", filter='"
                + filter
                + '\''
                + ", fetchSize="
                + fetchSize
                + ", writeMode='"
                + writeMode
                + '\''
                + ", replaceKey='"
                + replaceKey
                + '\''
                + ", mongodbConfig="
                + mongodbConfig
                + '}';
    }
}
