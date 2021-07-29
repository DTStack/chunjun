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
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.kudu.client.AsyncKuduClient;

import java.util.List;
import java.util.Map;

import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_ADMIN_OPERATION_TIMEOUT;
import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_AUTHENTICATION;
import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_BATCH_SIZE_BYTES;
import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_BOSS_COUNT;
import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_FILTER;
import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_KEYTABFILE;
import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_MASTER_ADDRESSES;
import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_OPERATION_TIMEOUT;
import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_PRINCIPAL;
import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_QUERY_TIMEOUT;
import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_READ_MODE;
import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_TABLE;
import static com.dtstack.flinkx.kudu.core.KuduConfigKeys.KEY_WORKER_COUNT;

/**
 * @author jiangbo
 * @date 2019/7/31
 */
public class KuduReader extends BaseDataReader {

    private List<MetaColumn> columns;

    private KuduConfig kuduConfig;

    protected Map<String,Object> hadoopConfig;

    public KuduReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        ReaderConfig.ParameterConfig parameterConfig = readerConfig.getParameter();

        columns = MetaColumn.getMetaColumns(parameterConfig.getColumn());
        kuduConfig = KuduConfigBuilder.getInstance()
                .withMasterAddresses(parameterConfig.getStringVal(KEY_MASTER_ADDRESSES))
                .withAuthentication(parameterConfig.getStringVal(KEY_AUTHENTICATION))
                .withprincipal(parameterConfig.getStringVal(KEY_PRINCIPAL))
                .withKeytabFile(parameterConfig.getStringVal(KEY_KEYTABFILE))
                .withWorkerCount(parameterConfig.getIntVal(KEY_WORKER_COUNT, 2 * Runtime.getRuntime().availableProcessors()))
                .withBossCount(parameterConfig.getIntVal(KEY_BOSS_COUNT, 1))
                .withOperationTimeout(parameterConfig.getLongVal(KEY_OPERATION_TIMEOUT, AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS))
                .withQueryTimeout(parameterConfig.getLongVal(KEY_QUERY_TIMEOUT, AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS))
                .withAdminOperationTimeout(parameterConfig.getLongVal(KEY_ADMIN_OPERATION_TIMEOUT, AsyncKuduClient.DEFAULT_KEEP_ALIVE_PERIOD_MS))
                .withTable(parameterConfig.getStringVal(KEY_TABLE))
                .withReadMode(parameterConfig.getStringVal(KEY_READ_MODE))
                .withBatchSizeBytes(parameterConfig.getIntVal(KEY_BATCH_SIZE_BYTES, 1024*1024))
                .withFilter(parameterConfig.getStringVal(KEY_FILTER))
                .build();

        hadoopConfig = (Map<String, Object>) readerConfig.getParameter().getVal("hadoopConfig");
    }

    @Override
    public DataStream<Row> readData() {
        KuduInputFormatBuilder builder = new KuduInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setColumns(columns);
        builder.setMonitorUrls(monitorUrls);
        builder.setBytes(bytes);
        builder.setKuduConfig(kuduConfig);
        builder.setTestConfig(testConfig);
        builder.setLogConfig(logConfig);
        builder.setHadoopConfig(hadoopConfig);

        return createInput(builder.finish());
    }
}
