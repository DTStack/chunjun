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

package com.dtstack.chunjun.connector.mongodb.conf;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.ServerAddress;

import java.io.Serializable;
import java.util.List;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/21
 */
public class MongoClientConf implements Serializable {

    private String uri;
    private String database;
    private String collection;
    private List<ServerAddress> serverAddresses;
    private String username;
    private String password;
    private String authenticationMechanism = AuthenticationMechanism.SCRAM_SHA_1.getMechanismName();
    private MongodbClientOptions mongodbClientOptions;

    public String getAuthenticationMechanism() {
        return authenticationMechanism;
    }

    public void setAuthenticationMechanism(String authenticationMechanism) {
        this.authenticationMechanism = authenticationMechanism;
    }

    public MongodbClientOptions getConnectionConfig() {
        return mongodbClientOptions;
    }

    public void setConnectionConfig(MongodbClientOptions mongodbClientOptions) {
        this.mongodbClientOptions = mongodbClientOptions;
    }

    public List<ServerAddress> getServerAddresses() {
        return serverAddresses;
    }

    public void setServerAddresses(List<ServerAddress> serverAddresses) {
        this.serverAddresses = serverAddresses;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getCollection() {
        return collection;
    }

    public void setCollection(String collection) {
        this.collection = collection;
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
}
