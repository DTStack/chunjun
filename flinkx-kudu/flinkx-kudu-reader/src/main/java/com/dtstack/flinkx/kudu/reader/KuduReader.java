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


package com.dtstack.flinkx.kudu.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.kudu.core.KuduConfig;
import com.dtstack.flinkx.kudu.core.KuduConfigBuilder;
import com.dtstack.flinkx.reader.DataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.kudu.client.AsyncKuduClient;

import java.util.List;

/**
 * @author jiangbo
 * @date 2019/7/31
 */
public class KuduReader extends DataReader {

    private String table;

    private List<MetaColumn> columns;

    private KuduConfig kuduConfig;

    private String readMode;

    private String filterString;

    protected KuduReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        ReaderConfig.ParameterConfig parameterConfig = readerConfig.getParameter();

        columns = MetaColumn.getMetaColumns(parameterConfig.getColumn());
        table = parameterConfig.getStringVal("table");
        readMode = parameterConfig.getStringVal("readMode");
        filterString = parameterConfig.getStringVal("filter");
        kuduConfig = KuduConfigBuilder.getInstance()
                .withMasterAddresses(parameterConfig.getStringVal("masterAddresses"))
                .withOpenKerberos(parameterConfig.getBooleanVal("openKerberos", false))
                .withUser(parameterConfig.getStringVal("user"))
                .withKeytabPath(parameterConfig.getStringVal("keytabPath"))
                .withWorkerCount(parameterConfig.getIntVal("workerCount", 2 * Runtime.getRuntime().availableProcessors()))
                .withBossCount(parameterConfig.getIntVal("bossCount", 1))
                .withOperationTimeout(parameterConfig.getLongVal("operationTimeout", AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS))
                .withAdminOperationTimeout(parameterConfig.getLongVal("adminOperationTimeout", AsyncKuduClient.DEFAULT_KEEP_ALIVE_PERIOD_MS))
                .build();
    }

    @Override
    public DataStream<Row> readData() {
        KuduInputFormatBuilder builder = new KuduInputFormatBuilder();
        builder.setColumns(columns);
        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);
        builder.setTable(table);
        builder.setReadMode(readMode);
        builder.setKuduConfig(kuduConfig);
        builder.setFilterString(filterString);

        return createInput(builder.finish(), "kudureader");
    }
}
