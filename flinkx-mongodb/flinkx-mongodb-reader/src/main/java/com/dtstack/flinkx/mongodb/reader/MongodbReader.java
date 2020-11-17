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
import com.dtstack.flinkx.mongodb.MongodbConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * The Reader plugin for mongodb database
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class MongodbReader extends BaseDataReader {

    private List<MetaColumn> metaColumns;

    private MongodbConfig mongodbConfig;

    public MongodbReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn());

        try {
            mongodbConfig = objectMapper.readValue(objectMapper.writeValueAsString(readerConfig.getParameter().getAll()), MongodbConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("解析mongodb配置出错:", e);
        }
    }

    @Override
    public DataStream<Row> readData() {
        MongodbInputFormatBuilder builder = new MongodbInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setMetaColumns(metaColumns);
        builder.setMongodbConfig(mongodbConfig);

        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);
        builder.setTestConfig(testConfig);

        return createInput(builder.finish());
    }
}
