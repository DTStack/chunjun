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

package com.dtstack.flinkx.mongodb;

import com.dtstack.flinkx.enums.ColType;
import com.dtstack.flinkx.exception.WriteRecordException;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.util.DateUtil;
import com.dtstack.flinkx.util.TelnetUtil;
import com.google.common.collect.Lists;
import com.mongodb.*;
import com.mongodb.client.MongoCursor;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.types.Row;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.dtstack.flinkx.mongodb.MongodbConfigKeys.*;

/**
 * Utilities for mongodb database connection and data format conversion
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class MongodbUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbUtil.class);

    private static final String HOST_SPLIT_REGEX = ",\\s*";

    private static Pattern HOST_PORT_PATTERN = Pattern.compile("(?<host>.*):(?<port>\\d+)*");

    private static final Integer DEFAULT_PORT = 27017;

    private static final Integer DEFAULT_CONNECTIONS_PER_HOST = 100;

    private static final Integer DEFAULT_THREADS_FOR_CONNECTION_MULTIPLIER = 100;

    private static final Integer DEFAULT_CONNECT_TIMEOUT = 10 * 1000;

    private static final Integer DEFAULT_MAX_WAIT_TIME = 5 * 1000;

    private static  final Integer DEFAULT_SOCKET_TIMEOUT = 0;

    /**
     * Get mongo client
     * @param mongodbConfig
     * @return MongoClient
     */
    public static MongoClient getMongoClient(Map<String,Object> mongodbConfig){
        MongoClient mongoClient;
        try{
            MongoClientOptions options = getOption(mongodbConfig);
            List<ServerAddress> serverAddress = getServerAddress(MapUtils.getString(mongodbConfig, KEY_HOST_PORTS));
            String username = MapUtils.getString(mongodbConfig, KEY_USERNAME);
            String password = MapUtils.getString(mongodbConfig, KEY_PASSWORD);
            String database = MapUtils.getString(mongodbConfig, KEY_DATABASE);

            if(StringUtils.isEmpty(username)){
                mongoClient = new MongoClient(serverAddress,options);
            } else {
                MongoCredential credential = MongoCredential.createScramSha1Credential(username, database, password.toCharArray());
                List<MongoCredential> credentials = Lists.newArrayList();
                credentials.add(credential);

                mongoClient = new MongoClient(serverAddress,credentials,options);
            }

            LOG.info("Get mongodb client successful");
            return mongoClient;
        }catch (Exception e){
            throw new RuntimeException(e);
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

    public static Document convertRowToDoc(Row row,List<MetaColumn> columns) throws WriteRecordException {
        Document doc = new Document();
        for (int i = 0; i < columns.size(); i++) {
            MetaColumn column = columns.get(i);
            Object val = convertField(row.getField(i),column);
            if (StringUtils.isNotEmpty(column.getSplitter())){
                val = Arrays.asList(String.valueOf(val).split(column.getSplitter()));
            }

            doc.append(column.getName(),val);
        }

        return doc;
    }

    private static Object convertField(Object val,MetaColumn column){
        if(val instanceof BigDecimal){
           val = ((BigDecimal) val).doubleValue();
        }

        if (val instanceof Timestamp && !column.getType().equalsIgnoreCase(ColType.INTEGER.toString())){
            SimpleDateFormat format = DateUtil.getDateTimeFormatter();
            val= format.format(val);
        }

        return val;
    }

    /**
     * parse server address from hostPorts string
     */
    private static List<ServerAddress> getServerAddress(String hostPorts) {
        List<ServerAddress> addresses = Lists.newArrayList();

        for (String hostPort : hostPorts.split(HOST_SPLIT_REGEX)) {
            if(hostPort.length() == 0){
                continue;
            }

            Matcher matcher = HOST_PORT_PATTERN.matcher(hostPort);
            if(matcher.find()){
                String host = matcher.group("host");
                String portStr = matcher.group("port");
                int port = portStr == null ? DEFAULT_PORT : Integer.parseInt(portStr);

                TelnetUtil.telnet(host,port);

                ServerAddress serverAddress = new ServerAddress(host,port);
                addresses.add(serverAddress);
            }
        }

        return addresses;
    }

    private static MongoClientOptions getOption(Map<String,Object> mongodbConfig){
        MongoClientOptions.Builder build = new MongoClientOptions.Builder();

        int connectionsPerHost = MapUtils.getIntValue(mongodbConfig, KEY_CONNECTIONS_PERHOST, DEFAULT_CONNECTIONS_PER_HOST);
        LOG.info("Mongodb config -- connectionsPerHost:" + connectionsPerHost);
        build.connectionsPerHost(connectionsPerHost);

        int threadsForConnectionMultiplier = MapUtils.getIntValue(mongodbConfig, KEY_THREADS_FOR_CONNECTION_MULTIPLIER, DEFAULT_THREADS_FOR_CONNECTION_MULTIPLIER);
        LOG.info("Mongodb config -- threadsForConnectionMultiplier:" + threadsForConnectionMultiplier);
        build.threadsAllowedToBlockForConnectionMultiplier(threadsForConnectionMultiplier);

        int connectionTimeout = MapUtils.getIntValue(mongodbConfig, KEY_CONNECTION_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
        LOG.info("Mongodb config -- connectionTimeout:" + connectionTimeout);
        build.connectTimeout(connectionTimeout);

        int maxWaitTime = MapUtils.getIntValue(mongodbConfig, KEY_MAX_WAIT_TIME, DEFAULT_MAX_WAIT_TIME);
        LOG.info("Mongodb config -- maxWaitTime:" + maxWaitTime);
        build.maxWaitTime(maxWaitTime);

        int socketTimeout = MapUtils.getIntValue(mongodbConfig, KEY_SOCKET_TIMEOUT, DEFAULT_SOCKET_TIMEOUT);
        LOG.info("Mongodb config -- socketTimeout:" + socketTimeout);
        build.maxWaitTime(socketTimeout);

        build.writeConcern(WriteConcern.UNACKNOWLEDGED);
        return build.build();
    }
}
