/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flinkx.binlog.reader;

import com.dtstack.flinkx.binlog.format.BinlogInputFormatBuilder;
import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @company: www.dtstack.com
 * @author: toutian
 * @create: 2019/7/4
 */
public class BinlogReader extends BaseDataReader {

    private BinlogConfig binlogConfig;

    @SuppressWarnings("unchecked")
    public BinlogReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

        try {
            binlogConfig = objectMapper.readValue(objectMapper.writeValueAsString(readerConfig.getParameter().getAll()), BinlogConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("解析binlog Config配置出错:", e);
        }
    }

    @Override
    public DataStream<Row> readData() {

        BinlogInputFormatBuilder builder = new BinlogInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setBinlogConfig(binlogConfig);
        builder.setRestoreConfig(restoreConfig);
        builder.setLogConfig(logConfig);
        builder.setTestConfig(testConfig);
        return createInput(builder.finish());
    }

}
