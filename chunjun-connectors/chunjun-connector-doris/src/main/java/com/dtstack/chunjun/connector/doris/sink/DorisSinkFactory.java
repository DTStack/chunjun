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

package com.dtstack.chunjun.connector.doris.sink;

import com.dtstack.chunjun.conf.OperatorConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.doris.options.DorisConf;
import com.dtstack.chunjun.connector.doris.options.DorisConfBuilder;
import com.dtstack.chunjun.connector.doris.options.LoadConf;
import com.dtstack.chunjun.connector.doris.options.LoadConfBuilder;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import java.util.List;
import java.util.Properties;

import static com.dtstack.chunjun.connector.doris.options.DorisKeys.BATCH_SIZE_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DATABASE_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DESERIALIZE_ARROW_ASYNC_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DESERIALIZE_QUEUE_SIZE_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_BATCH_SIZE_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_EXEC_MEM_LIMIT_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_REQUEST_RETRIES_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.DORIS_WRITE_MODE_DEFAULT;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.EXEC_MEM_LIMIT_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.FE_NODES_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.FLUSH_INTERNAL_MS_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.LOAD_OPTIONS_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.LOAD_PROPERTIES_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.PASSWORD_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.REQUEST_BATCH_SIZE_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.REQUEST_CONNECT_TIMEOUT_MS_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.REQUEST_QUERY_TIMEOUT_S_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.REQUEST_READ_TIMEOUT_MS_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.REQUEST_RETRIES_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.REQUEST_TABLET_SIZE_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.TABLE_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.USER_NAME_KEY;
import static com.dtstack.chunjun.connector.doris.options.DorisKeys.WRITE_MODE_KEY;

/**
 * Company：www.dtstack.com.
 *
 * @author shitou
 * @date 2021/11/8
 */
public class DorisSinkFactory extends SinkFactory {

    private final DorisConf options;

    public DorisSinkFactory(SyncConf syncConf) {
        super(syncConf);

        final OperatorConf parameter = syncConf.getWriter();

        DorisConfBuilder dorisConfBuilder = new DorisConfBuilder();
        LoadConfBuilder loadConfBuilder = new LoadConfBuilder();

        Properties properties = parameter.getProperties(LOAD_OPTIONS_KEY, new Properties());
        LoadConf loadConf =
                loadConfBuilder
                        .setRequestTabletSize(
                                (int)
                                        properties.getOrDefault(
                                                REQUEST_TABLET_SIZE_KEY, Integer.MAX_VALUE))
                        .setRequestConnectTimeoutMs(
                                (int)
                                        properties.getOrDefault(
                                                REQUEST_CONNECT_TIMEOUT_MS_KEY,
                                                DORIS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT))
                        .setRequestReadTimeoutMs(
                                (int)
                                        properties.getOrDefault(
                                                REQUEST_READ_TIMEOUT_MS_KEY,
                                                DORIS_REQUEST_READ_TIMEOUT_MS_DEFAULT))
                        .setRequestQueryTimeoutMs(
                                (int)
                                        properties.getOrDefault(
                                                REQUEST_QUERY_TIMEOUT_S_KEY,
                                                DORIS_REQUEST_QUERY_TIMEOUT_S_DEFAULT))
                        .setRequestRetries(
                                (int)
                                        properties.getOrDefault(
                                                REQUEST_RETRIES_KEY, DORIS_REQUEST_RETRIES_DEFAULT))
                        .setRequestBatchSize(
                                (int)
                                        properties.getOrDefault(
                                                REQUEST_BATCH_SIZE_KEY, DORIS_BATCH_SIZE_DEFAULT))
                        .setExecMemLimit(
                                (long)
                                        properties.getOrDefault(
                                                EXEC_MEM_LIMIT_KEY, DORIS_EXEC_MEM_LIMIT_DEFAULT))
                        .setDeserializeQueueSize(
                                (int)
                                        properties.getOrDefault(
                                                DESERIALIZE_QUEUE_SIZE_KEY,
                                                DORIS_DESERIALIZE_QUEUE_SIZE_DEFAULT))
                        .setDeserializeArrowAsync(
                                (boolean)
                                        properties.getOrDefault(
                                                DESERIALIZE_ARROW_ASYNC_KEY,
                                                DORIS_DESERIALIZE_ARROW_ASYNC_DEFAULT))
                        .build();

        options =
                dorisConfBuilder
                        .setDatabase(parameter.getStringVal(DATABASE_KEY))
                        .setTable(parameter.getStringVal(TABLE_KEY))
                        .setFeNodes((List<String>) parameter.getVal(FE_NODES_KEY))
                        .setLoadOptions(loadConf)
                        .setLoadProperties(
                                parameter.getProperties(LOAD_PROPERTIES_KEY, new Properties()))
                        .setPassword(parameter.getStringVal(PASSWORD_KEY, ""))
                        .setNameMapped(
                                syncConf.getNameMappingConf() != null
                                        && !syncConf.getNameMappingConf().isEmpty())
                        .setWriteMode(
                                parameter.getStringVal(WRITE_MODE_KEY, DORIS_WRITE_MODE_DEFAULT))
                        .setUsername(parameter.getStringVal(USER_NAME_KEY))
                        .setBatchSize(parameter.getIntVal(BATCH_SIZE_KEY, 1000))
                        .setFlushIntervalMills(parameter.getLongVal(FLUSH_INTERNAL_MS_KEY, 10000L))
                        .build();
        options.setColumn(syncConf.getWriter().getFieldList());
        super.initCommonConf(options);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        DorisOutputFormatBuilder builder = new DorisOutputFormatBuilder();
        builder.setDorisOptions(options);
        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return null;
    }
}
