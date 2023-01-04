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

import com.dtstack.chunjun.connector.mongodb.config.MongoClientConfig;

import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MongoClientConfFactory {

    private static final String HOST_SPLIT_REGEX = ",\\s*";
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("(?<host>.*):(?<port>\\d+)*");
    private static final Integer DEFAULT_PORT = 27017;

    public static MongoClientConfig createMongoClientConf(
            MongodbDataSyncConfig mongodbDataSyncConfig) {
        MongoClientConfig mongoClientConfig = new MongoClientConfig();
        mongoClientConfig.setUri(mongodbDataSyncConfig.getUrl());
        mongoClientConfig.setUsername(mongodbDataSyncConfig.getUsername());
        mongoClientConfig.setPassword(mongodbDataSyncConfig.getPassword());
        mongoClientConfig.setCollection(mongodbDataSyncConfig.getCollectionName());

        mongoClientConfig.setDatabase(mongodbDataSyncConfig.getDatabase());
        if (mongodbDataSyncConfig.getUrl() != null) {
            MongoClientURI clientUri = new MongoClientURI(mongodbDataSyncConfig.getUrl());
            mongoClientConfig.setDatabase(clientUri.getDatabase());
        }

        if (mongodbDataSyncConfig.getHostPorts() != null) {
            mongoClientConfig.setServerAddresses(
                    parseServerAddress(mongodbDataSyncConfig.getHostPorts()));
        }
        mongoClientConfig.setAuthenticationMechanism(
                mongodbDataSyncConfig.getAuthenticationMechanism());
        mongoClientConfig.setMongodbClientOptions(mongodbDataSyncConfig.getMongodbConfig());
        return mongoClientConfig;
    }

    /** parse server address from hostPorts string */
    private static List<ServerAddress> parseServerAddress(String hostPorts) {
        List<ServerAddress> addresses = new ArrayList<>();

        for (String hostPort : hostPorts.split(HOST_SPLIT_REGEX)) {
            if (hostPort.length() == 0) {
                continue;
            }

            Matcher matcher = HOST_PORT_PATTERN.matcher(hostPort);
            if (matcher.find()) {
                String host = matcher.group("host");
                String portStr = matcher.group("port");
                int port = portStr == null ? DEFAULT_PORT : Integer.parseInt(portStr);

                ServerAddress serverAddress = new ServerAddress(host, port);
                addresses.add(serverAddress);
            }
        }
        return addresses;
    }
}
