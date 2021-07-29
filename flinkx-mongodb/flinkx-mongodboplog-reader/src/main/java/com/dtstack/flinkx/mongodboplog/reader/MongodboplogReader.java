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


package com.dtstack.flinkx.mongodboplog.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.mongodb.MongodbConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.mongodboplog.format.MongodboplogInputFormatBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author jiangbo
 * @date 2019/12/5
 */
public class MongodboplogReader extends BaseDataReader {

    private MongodbConfig mongodbConfig;

    public MongodboplogReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        try {
            mongodbConfig = objectMapper.readValue(objectMapper.writeValueAsString(readerConfig.getParameter().getAll()), MongodbConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("parse mongodb config error:", e);
        }
    }

    @Override
    public DataStream<Row> readData() {
        MongodboplogInputFormatBuilder builder = new MongodboplogInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setMongodbConfig(mongodbConfig);
        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);
        builder.setRestoreConfig(restoreConfig);
        builder.setLogConfig(logConfig);
        builder.setTestConfig(testConfig);
        return createInput(builder.finish());
    }
}
