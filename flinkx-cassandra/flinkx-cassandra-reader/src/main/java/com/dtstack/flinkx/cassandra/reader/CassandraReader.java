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
import com.dtstack.flinkx.reader.DataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;

import static com.dtstack.flinkx.cassandra.CassandraConfigKeys.*;

/**
 * The Reader plugin for cassandra
 *
 * @Company: www.dtstack.com
 * @author wuhui
 */
public class CassandraReader extends DataReader {

    protected String hostPorts;

    protected String username;

    protected String password;

    protected String url;

    protected String keySpace;

    protected String table;

    protected Map<String,Object> cassandraConfig;


    public CassandraReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        hostPorts = readerConfig.getParameter().getStringVal(KEY_HOST_PORTS);
        username = readerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = readerConfig.getParameter().getStringVal(KEY_PASSWORD);
        url = readerConfig.getParameter().getStringVal(KEY_URL);
        keySpace = readerConfig.getParameter().getStringVal(KEY_KEY_SPACE);
        table = readerConfig.getParameter().getStringVal(KEY_TABLE);

        cassandraConfig = (Map<String,Object>)readerConfig.getParameter().getVal(KEY_CASSANDRA_CONFIG, new HashMap<>());
        cassandraConfig.put(KEY_HOST_PORTS, hostPorts);
        cassandraConfig.put(KEY_USERNAME, username);
        cassandraConfig.put(KEY_PASSWORD, password);
        cassandraConfig.put(KEY_URL, url);
        cassandraConfig.put(KEY_KEY_SPACE, keySpace);
        cassandraConfig.put(KEY_TABLE, table);
    }

    @Override
    public DataStream<Row> readData() {
        CassandraInputFormatBuilder builder = new CassandraInputFormatBuilder();
        builder.setHostPorts(hostPorts);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setTable(table);
        builder.setCassandraConfig(cassandraConfig);

        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);

        return createInput(builder.finish(),"cassandrareader");
    }
}
