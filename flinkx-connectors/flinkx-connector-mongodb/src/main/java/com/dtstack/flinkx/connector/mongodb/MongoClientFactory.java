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

package com.dtstack.flinkx.connector.mongodb;

import com.dtstack.flinkx.connector.mongodb.conf.MongoClientConf;
import com.dtstack.flinkx.connector.mongodb.conf.MongodbClientOptions;

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Ada Wong
 * @program flinkx
 * @create 2021/06/22
 */
public class MongoClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MongoClientFactory.class);

    private static final String HOST_SPLIT_REGEX = ",\\s*";
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("(?<host>.*):(?<port>\\d+)*");
    private static final Integer DEFAULT_PORT = 27017;

    public static MongoClient createClientWithUri(String uri) {
        MongoClientURI clientUri = new MongoClientURI(uri);
        return new MongoClient(clientUri);
    }

    public static MongoClient createClient(MongoClientConf mongoClientConf) {
        String uri = mongoClientConf.getUri();
        if (null != uri) {
            return createClientWithUri(mongoClientConf.getUri());
        } else {
            String username = mongoClientConf.getUsername();
            List<ServerAddress> serverAddresses = mongoClientConf.getServerAddresses();
            MongoClientOptions options =
                    MongodbClientOptions.getClientOptionsWhenDataSync(
                            mongoClientConf.getConnectionConfig());
            if (StringUtils.isNotEmpty(username)) {
                MongoCredential credential =
                        createMongoCredential(
                                mongoClientConf.getDatabase(),
                                username,
                                mongoClientConf.getPassword(),
                                mongoClientConf.getAuthenticationMechanism());
                return new MongoClient(serverAddresses, credential, options);
            } else {
                return new MongoClient(serverAddresses, options);
            }
        }
    }

    private static MongoCredential createMongoCredential(
            String database, String username, String password, String authenticationMechanism) {
        MongoCredential credential =
                MongoCredential.createCredential(username, database, password.toCharArray())
                        .withMechanism(
                                AuthenticationMechanism.fromMechanismName(authenticationMechanism));
        return credential;
    }

    public static MongoCollection<Document> createCollection(
            MongoClient client, String databaseName, String collectionName) {
        MongoDatabase database = client.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        return collection;
    }

    /** parse server address from hostPorts string */
    public static List<ServerAddress> getServerAddress(String hostPorts) {
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
