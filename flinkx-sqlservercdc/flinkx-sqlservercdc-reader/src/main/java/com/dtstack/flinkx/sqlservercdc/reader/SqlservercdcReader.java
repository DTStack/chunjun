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
package com.dtstack.flinkx.sqlservercdc.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.sqlservercdc.SqlServerCdcConfigKeys;
import com.dtstack.flinkx.sqlservercdc.format.SqlserverCdcInputFormatBuilder;
import com.dtstack.flinkx.util.StringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Date: 2019/12/03
 * Company: www.dtstack.com
 *
 * @author tudou
 */
public class SqlservercdcReader extends BaseDataReader {
    private String username;
    private String password;
    private String url;
    private String databaseName;
    private String cat;
    private boolean pavingData;
    private List<String> tableList;
    private Long pollInterval;
    private String lsn;

    @SuppressWarnings("unchecked")
    public SqlservercdcReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        username = readerConfig.getParameter().getStringVal(SqlServerCdcConfigKeys.KEY_USER_NAME);
        password = readerConfig.getParameter().getStringVal(SqlServerCdcConfigKeys.KEY_PASSWORD);
        url = readerConfig.getParameter().getStringVal(SqlServerCdcConfigKeys.KEY_URL);
        databaseName = readerConfig.getParameter().getStringVal(SqlServerCdcConfigKeys.KEY_DATABASE_NAME);
        cat = readerConfig.getParameter().getStringVal(SqlServerCdcConfigKeys.KEY_CATALOG);
        pavingData = readerConfig.getParameter().getBooleanVal(SqlServerCdcConfigKeys.KEY_PAVING_DATA, false);
        List<String> tables = (List<String>) readerConfig.getParameter().getVal(SqlServerCdcConfigKeys.KEY_TABLE_LIST);

        if (CollectionUtils.isNotEmpty(tables)) {
            tableList = new ArrayList<>(tables.size());
            //兼容[].[]
            tables.forEach(item -> tableList.add(StringUtil.splitIgnoreQuotaAndJoinByPoint(item)));
        } else {
            tableList = Collections.emptyList();
        }

        pollInterval = readerConfig.getLongVal(SqlServerCdcConfigKeys.KEY_POLL_INTERVAL, 1000);
        lsn = readerConfig.getParameter().getStringVal(SqlServerCdcConfigKeys.KEY_LSN);
    }

    @Override
    public DataStream<Row> readData() {
        SqlserverCdcInputFormatBuilder builder = new SqlserverCdcInputFormatBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setUrl(url);
        builder.setDatabaseName(databaseName);
        builder.setCat(cat);
        builder.setPavingData(pavingData);
        builder.setTable(tableList);
        builder.setRestoreConfig(restoreConfig);
        builder.setPollInterval(pollInterval);
        builder.setLsn(lsn);

        return createInput(builder.finish(), "sqlserverdcreader");
    }
}
