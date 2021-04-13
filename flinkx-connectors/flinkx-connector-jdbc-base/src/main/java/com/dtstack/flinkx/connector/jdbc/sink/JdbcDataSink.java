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
package com.dtstack.flinkx.connector.jdbc.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;

import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.connector.jdbc.DtJdbcDialect;
import com.dtstack.flinkx.connector.jdbc.adapter.ConnectionAdapter;
import com.dtstack.flinkx.connector.jdbc.conf.ConnectionConf;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.outputformat.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.sink.BaseDataSink;
import com.dtstack.flinkx.util.GsonUtil;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.util.Properties;

/**
 * Date: 2021/04/13
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public abstract class JdbcDataSink extends BaseDataSink {
    protected JdbcConf jdbcConf;
    protected DtJdbcDialect dtJdbcDialect;

    public JdbcDataSink(SyncConf syncConf) {
        super(syncConf);
        Gson gson = new GsonBuilder().registerTypeAdapter(ConnectionConf.class, new ConnectionAdapter("SinkConnectionConf")).create();
        GsonUtil.setTypeAdapter(gson);
        jdbcConf = gson.fromJson(gson.toJson(syncConf.getWriter().getParameter()), JdbcConf.class);
        jdbcConf.setColumn(syncConf.getWriter().getFieldList());
        Properties properties = syncConf.getWriter().getProperties("properties", null);
        jdbcConf.setProperties(properties);
        super.initFlinkxCommonConf(jdbcConf);
    }

    @Override
    public DataStreamSink<RowData> writeData(DataStream<RowData> dataSet) {
        JdbcOutputFormatBuilder builder = getBuilder();

        builder.setJdbcConf(jdbcConf);
        builder.setDtJdbcDialect(dtJdbcDialect);
        builder.setBatchSize(jdbcConf.getBatchSize());
        return createOutput(dataSet, builder.finish());
    }

    /**
     * 获取JDBC插件的具体outputFormatBuilder
     * @return JdbcOutputFormatBuilder
     */
    protected abstract JdbcOutputFormatBuilder getBuilder();
}
