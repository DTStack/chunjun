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
package com.dtstack.flinkx.cassandra.writer;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.WriterConfig;
import com.dtstack.flinkx.reader.MetaColumn;
import com.dtstack.flinkx.writer.BaseDataWriter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.cassandra.CassandraConfigKeys.*;

/**
 *
 * @Company: www.dtstack.com
 * @author wuhui
 */
public class CassandraWriter extends BaseDataWriter {

    protected String host;

    protected Integer port;

    protected String username;

    protected String password;

    protected boolean useSSL;

    protected String keySpace;

    protected String table;

    protected List<MetaColumn> column;

    protected String consistancyLevel;

    protected int connectionsPerHost;

    protected int maxPendingPerConnection;

    protected boolean asyncWrite;

    protected Long batchSize;

    protected Map<String,Object> cassandraConfig;


    public CassandraWriter(DataTransferConfig config) {
        super(config);
        WriterConfig writerConfig = config.getJob().getContent().get(0).getWriter();
        host = writerConfig.getParameter().getStringVal(KEY_HOST);
        port = writerConfig.getParameter().getIntVal(KEY_PORT, 9042);
        username = writerConfig.getParameter().getStringVal(KEY_USERNAME);
        password = writerConfig.getParameter().getStringVal(KEY_PASSWORD);
        useSSL = writerConfig.getParameter().getBooleanVal(KEY_USE_SSL, false);
        column = MetaColumn.getMetaColumns(writerConfig.getParameter().getColumn());
        keySpace = writerConfig.getParameter().getStringVal(KEY_KEY_SPACE);
        table = writerConfig.getParameter().getStringVal(KEY_TABLE);
        connectionsPerHost = writerConfig.getParameter().getIntVal(KEY_CONNECTION_PER_HOST, 8);
        maxPendingPerConnection = writerConfig.getParameter().getIntVal(KEY_MAX_PENDING_CONNECTION, 128);
        asyncWrite = writerConfig.getParameter().getBooleanVal(KEY_ASYNC_WRITE, false);
        batchSize = writerConfig.getParameter().getLongVal(KEY_BATCH_SIZE, 1);
        consistancyLevel = writerConfig.getParameter().getStringVal(KEY_CONSITANCY_LEVEL);

        cassandraConfig = (Map<String,Object>)writerConfig.getParameter().getVal(KEY_CASSANDRA_CONFIG, new HashMap<>());
        cassandraConfig.put(KEY_HOST, host);
        cassandraConfig.put(KEY_PORT, port);
        cassandraConfig.put(KEY_USERNAME, username);
        cassandraConfig.put(KEY_PASSWORD, password);
        cassandraConfig.put(KEY_USE_SSL, useSSL);
        cassandraConfig.put(KEY_COLUMN, column);
        cassandraConfig.put(KEY_KEY_SPACE, keySpace);
        cassandraConfig.put(KEY_TABLE, table);
        cassandraConfig.put(KEY_CONNECTION_PER_HOST, connectionsPerHost);
        cassandraConfig.put(KEY_MAX_PENDING_CONNECTION, maxPendingPerConnection);
        cassandraConfig.put(KEY_ASYNC_WRITE, asyncWrite);
        cassandraConfig.put(KEY_BATCH_SIZE, batchSize);
        cassandraConfig.put(KEY_CONSITANCY_LEVEL, consistancyLevel);
    }

    @Override
    public DataStreamSink<?> writeData(DataStream<Row> dataSet) {
        CassandraOutputFormatBuilder builder = new CassandraOutputFormatBuilder();
        builder.setKeySpace(keySpace);
        builder.setTable(table);
        builder.setColumn(column);
        builder.setAsyncWrite(asyncWrite);
        builder.setBatchSize(batchSize);
        builder.setConsistancyLevel(consistancyLevel);
        builder.setCassandraConfig(cassandraConfig);

        builder.setMonitorUrls(monitorUrls);
        builder.setErrors(errors);
        builder.setDirtyPath(dirtyPath);
        builder.setDirtyHadoopConfig(dirtyHadoopConfig);
        builder.setSrcCols(srcCols);
        return createOutput(dataSet, builder.finish(), "cassandrawriter");
    }
}
