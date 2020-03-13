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

import com.mongodb.*;
import com.mongodb.client.MongoCursor;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author jiangbo
 * @date 2019/12/5
 */
public class MongodbClientUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbClientUtil.class);

    private static final String HOST_SPLIT_REGEX = ",\\s*";

    private static Pattern HOST_PORT_PATTERN = Pattern.compile("(?<host>.*):(?<port>\\d+)*");

    private static final Integer DEFAULT_PORT = 27017;

    public static MongoClient getClient(MongodbConfig config){
        if(LOG.isDebugEnabled()){
            LOG.debug("连接配置:{}", config);
        }

        if (StringUtils.isNotEmpty(config.getUrl())) {
            return getClientWithUrl(config);
        } else {
            return getClientWithHostPort(config);
        }
    }

    public static void close(MongoClient mongoClient, MongoCursor<Document> cursor){
        if (cursor != null){
            LOG.info("Start close mongodb cursor");
            cursor.close();
            LOG.info("Close mongodb cursor successfully");
        }

        if (mongoClient != null){
            LOG.info("Start close mongodb client");
            mongoClient.close();
            LOG.info("Close mongodb client successfully");
        }
    }

    private static MongoClient getClientWithHostPort(MongodbConfig config){
        MongoClientOptions options = getOption(config.getMongodbConfig());
        List<ServerAddress> serverAddress = getServerAddress(config.getHostPorts());

        if (StringUtils.isEmpty(config.getUsername())) {
            return new MongoClient(serverAddress,options);
        }

        MongoCredential credential = MongoCredential.createCredential(config.getUsername(), config.getDatabase(), config.getPassword().toCharArray())
                .withMechanism(AuthenticationMechanism.fromMechanismName(config.getAuthenticationMechanism()));

        return new MongoClient(serverAddress,credential,options);
    }

    private static MongoClient getClientWithUrl(MongodbConfig config){
        MongoClientURI clientUri = new MongoClientURI(config.getUrl());
        config.setDatabase(clientUri.getDatabase());
        return new MongoClient(clientUri);
    }

    private static MongoClientOptions getOption(MongodbConfig.ConnectionConfig connectionConfig){
        MongoClientOptions.Builder build = new MongoClientOptions.Builder();
        build.connectionsPerHost(connectionConfig.getConnectionsPerHost());
        build.threadsAllowedToBlockForConnectionMultiplier(connectionConfig.getThreadsForConnectionMultiplier());
        build.connectTimeout(connectionConfig.getConnectionTimeout());
        build.maxWaitTime(connectionConfig.getMaxWaitTime());
        build.socketTimeout(connectionConfig.getSocketTimeout());
        build.writeConcern(WriteConcern.UNACKNOWLEDGED);
        return build.build();
    }

    /**
     * parse server address from hostPorts string
     */
    private static List<ServerAddress> getServerAddress(String hostPorts) {
        List<ServerAddress> addresses = new ArrayList<>();

        for (String hostPort : hostPorts.split(HOST_SPLIT_REGEX)) {
            if(hostPort.length() == 0){
                continue;
            }

            Matcher matcher = HOST_PORT_PATTERN.matcher(hostPort);
            if(matcher.find()){
                String host = matcher.group("host");
                String portStr = matcher.group("port");
                int port = portStr == null ? DEFAULT_PORT : Integer.parseInt(portStr);

                ServerAddress serverAddress = new ServerAddress(host,port);
                addresses.add(serverAddress);
            }
        }

        return addresses;
    }
}
