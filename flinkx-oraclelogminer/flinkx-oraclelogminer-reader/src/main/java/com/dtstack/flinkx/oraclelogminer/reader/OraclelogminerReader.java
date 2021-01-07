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


package com.dtstack.flinkx.oraclelogminer.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.oraclelogminer.format.LogMinerConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @author jiangbo
 * @date 2019/12/14
 */
public class OraclelogminerReader extends BaseDataReader {

    private LogMinerConfig logMinerConfig;

    public OraclelogminerReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();

        try {
            logMinerConfig = objectMapper.readValue(objectMapper.writeValueAsString(readerConfig.getParameter().getAll()), LogMinerConfig.class);
        } catch (Exception e) {
            throw new RuntimeException("parse logMiner config error:", e);
        }

        buildTableListenerRegex();
    }

    private void buildTableListenerRegex(){
        if (CollectionUtils.isEmpty(logMinerConfig.getTable())) {
            return;
        }

        String tableListener = StringUtils.join(logMinerConfig.getTable(), ",");
        logMinerConfig.setListenerTables(tableListener);
    }

    @Override
    public DataStream<Row> readData() {
        OracleLogMinerInputFormatBuilder builder = new OracleLogMinerInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setLogMinerConfig(logMinerConfig);
        builder.setRestoreConfig(restoreConfig);

        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);

        return createInput(builder.finish());
    }
}
