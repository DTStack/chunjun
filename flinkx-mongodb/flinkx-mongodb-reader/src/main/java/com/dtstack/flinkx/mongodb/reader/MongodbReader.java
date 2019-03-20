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

package com.dtstack.flinkx.mongodb.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.DataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.mongodb.MongodbConfigKeys.*;

/**
 * The Reader plugin for mongodb database
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class MongodbReader extends DataReader {

    protected String hostPorts;

    protected String username;

    protected String password;

    protected String database;

    protected String collection;

    private List<MetaColumn> metaColumns;

    protected String filter;

    protected Map<String,Object> mongodbConfig;

    protected int fetchSize;

    public MongodbReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        hostPorts = readerConfig.getParameter().getStringVal(KEY_HOST_PORTS);
        username = readerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = readerConfig.getParameter().getStringVal(KEY_PASSWORD);
        database = readerConfig.getParameter().getStringVal(KEY_DATABASE);
        collection = readerConfig.getParameter().getStringVal(KEY_COLLECTION);
        filter = readerConfig.getParameter().getStringVal(KEY_FILTER);
        fetchSize = readerConfig.getParameter().getIntVal(KEY_FETCH_SIZE, 100);
        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn());

        mongodbConfig = (Map<String,Object>)readerConfig.getParameter().getVal(KEY_MONGODB_CONFIG, new HashMap<>());
        mongodbConfig.put(KEY_HOST_PORTS, hostPorts);
        mongodbConfig.put(KEY_USERNAME, username);
        mongodbConfig.put(KEY_PASSWORD, password);
        mongodbConfig.put(KEY_DATABASE, database);
    }

    @Override
    public DataStream<Row> readData() {
        MongodbInputFormatBuilder builder = new MongodbInputFormatBuilder();
        builder.setHostPorts(hostPorts);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setDatabase(database);
        builder.setCollection(collection);
        builder.setFilter(filter);
        builder.setMetaColumns(metaColumns);
        builder.setMongodbConfig(mongodbConfig);
        builder.setFetchSize(fetchSize);

        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);

        return createInput(builder.finish(),"mongodbreader");
    }
}
