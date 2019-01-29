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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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

    private static final Integer ONE_SECOND = 1000;

    private static final Integer CONNECTIONS_PER_HOST = 100;

    private static final Integer THREADS_FOR_CONNECTION_MULTIPLIER = 100;

    private static final Integer CONNECT_TIMEOUT = 10 * ONE_SECOND;

    private static final Integer MAX_WAIT_TIME = 5 * ONE_SECOND;

    private static  final Integer SOCKET_TIMEOUT = 0;

    private static MongoClient mongoClient;

    /**
     * Get mongo client
     * @param config
     * @return MongoClient
     */
    public static MongoClient getMongoClient(Map<String,String> config){
        try{
            if(mongoClient == null){
                MongoClientOptions options = getOption();
                List<ServerAddress> serverAddress = getServerAddress(config.get(KEY_HOST_PORTS));
                String username = config.get(KEY_USERNAME);
                String password = config.get(KEY_PASSWORD);
                String database = config.get(KEY_DATABASE);

                if(StringUtils.isEmpty(username)){
                    mongoClient = new MongoClient(serverAddress,options);
                } else {
                    MongoCredential credential = MongoCredential.createScramSha1Credential(username, database, password.toCharArray());
                    List<MongoCredential> credentials = Lists.newArrayList();
                    credentials.add(credential);

                    mongoClient = new MongoClient(serverAddress,credentials,options);
                }


                LOG.info("mongo客户端获取成功");
            }
            return mongoClient;
        }catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static MongoDatabase getDatabase(Map<String,String> config,String database){
        MongoClient client = getMongoClient(config);
        return mongoClient.getDatabase(database);
    }

    public static MongoCollection<Document> getCollection(Map<String,String> config,String database, String collection){
        MongoClient client = getMongoClient(config);
        MongoDatabase db = client.getDatabase(database);

        return db.getCollection(collection);
    }

    public static void close(){
        if (mongoClient != null){
            mongoClient.close();
            mongoClient = null;
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

    private static MongoClientOptions getOption(){
        MongoClientOptions.Builder build = new MongoClientOptions.Builder();
        build.connectionsPerHost(CONNECTIONS_PER_HOST);
        build.threadsAllowedToBlockForConnectionMultiplier(THREADS_FOR_CONNECTION_MULTIPLIER);
        build.connectTimeout(CONNECT_TIMEOUT);
        build.maxWaitTime(MAX_WAIT_TIME);
        build.socketTimeout(SOCKET_TIMEOUT);
        build.writeConcern(WriteConcern.UNACKNOWLEDGED);
        return build.build();
    }
}
