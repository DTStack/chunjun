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

package com.dtstack.flinkx.cassandra.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.cassandra.CassandraConfigKeys.*;

/**
 * The Reader plugin for cassandra
 *
 * @Company: www.dtstack.com
 * @author wuhui
 */
public class CassandraReader extends BaseDataReader {

    protected String host;

    protected Integer port;

    protected String username;

    protected String password;

    protected boolean useSSL;

    protected String keySpace;

    protected String table;

    protected List<MetaColumn> column;

    protected String where;

    protected boolean allowFiltering;

    protected String consistancyLevel;

    protected int connectionsPerHost;

    protected int maxPendingPerConnection;

    protected Map<String,Object> cassandraConfig;


    public CassandraReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        host = readerConfig.getParameter().getStringVal(KEY_HOST);
        port = readerConfig.getParameter().getIntVal(KEY_PORT, 9042);
        username = readerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = readerConfig.getParameter().getStringVal(KEY_PASSWORD);
        useSSL = readerConfig.getParameter().getBooleanVal(KEY_USE_SSL, false);
        column = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn());
        where = readerConfig.getParameter().getStringVal(KEY_WHERE);
        keySpace = readerConfig.getParameter().getStringVal(KEY_KEY_SPACE);
        table = readerConfig.getParameter().getStringVal(KEY_TABLE);
        allowFiltering = readerConfig.getParameter().getBooleanVal(KEY_ALLOW_FILTERING, false);
        connectionsPerHost = readerConfig.getParameter().getIntVal(KEY_CONNECTION_PER_HOST, 8);
        maxPendingPerConnection = readerConfig.getParameter().getIntVal(KEY_MAX_PENDING_CONNECTION, 128);
        consistancyLevel = readerConfig.getParameter().getStringVal(KEY_CONSITANCY_LEVEL);

        cassandraConfig = (Map<String,Object>)readerConfig.getParameter().getVal(KEY_CASSANDRA_CONFIG, new HashMap<>());
        cassandraConfig.put(KEY_HOST, host);
        cassandraConfig.put(KEY_PORT, port);
        cassandraConfig.put(KEY_USERNAME, username);
        cassandraConfig.put(KEY_PASSWORD, password);
        cassandraConfig.put(KEY_USE_SSL, useSSL);
        cassandraConfig.put(KEY_COLUMN, column);
        cassandraConfig.put(KEY_WHERE, where);
        cassandraConfig.put(KEY_KEY_SPACE, keySpace);
        cassandraConfig.put(KEY_TABLE, table);
        cassandraConfig.put(KEY_ALLOW_FILTERING, allowFiltering);
        cassandraConfig.put(KEY_CONNECTION_PER_HOST, connectionsPerHost);
        cassandraConfig.put(KEY_MAX_PENDING_CONNECTION, maxPendingPerConnection);
        cassandraConfig.put(KEY_CONSITANCY_LEVEL, consistancyLevel);
    }

    @Override
    public DataStream<Row> readData() {
        CassandraInputFormatBuilder builder = new CassandraInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setTable(table);
        builder.setWhere(where);
        builder.setConsistancyLevel(consistancyLevel);
        builder.setAllowFiltering(allowFiltering);
        builder.setKeySpace(keySpace);
        builder.setColumn(column);
        builder.setCassandraConfig(cassandraConfig);

        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);

        return createInput(builder.finish(),"cassandrareader");
    }
}
