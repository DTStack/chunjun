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

package com.dtstack.flinkx.rdb.datareader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.rdb.DataSource;
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.inputformat.DistributedJdbcInputFormatBuilder;
import com.dtstack.flinkx.rdb.util.DbUtil;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * The Reader plugin for multiple databases that can be connected via JDBC.
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class DistributedJdbcDataReader extends BaseDataReader {

    protected DatabaseInterface databaseInterface;

    protected String username;

    protected String password;

    protected List<MetaColumn> metaColumns;

    protected String where;

    protected String splitKey;

    protected String pluginName;

    protected int fetchSize;

    protected int queryTimeOut;

    protected List<ReaderConfig.ParameterConfig.ConnectionConfig> connectionConfigs;

    private static String DISTRIBUTED_TAG = "d";

    protected DistributedJdbcDataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        username = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_USER_NAME);
        password = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_PASSWORD);
        where = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_WHERE);
        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn());
        splitKey = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_SPLIK_KEY);
        connectionConfigs = readerConfig.getParameter().getConnection();
        fetchSize = readerConfig.getParameter().getIntVal(JdbcConfigKeys.KEY_FETCH_SIZE,0);
        queryTimeOut = readerConfig.getParameter().getIntVal(JdbcConfigKeys.KEY_QUERY_TIME_OUT,0);
        pluginName = readerConfig.getName();
    }

    @Override
    public DataStream<Row> readData() {
        DistributedJdbcInputFormatBuilder builder = getBuilder();
        builder.setDataTransferConfig(dataTransferConfig);
        builder.setDrivername(databaseInterface.getDriverClass());
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setBytes(bytes);
        builder.setMonitorUrls(monitorUrls);
        builder.setDatabaseInterface(databaseInterface);
        builder.setMetaColumn(metaColumns);
        builder.setSourceList(buildConnections());
        builder.setNumPartitions(numPartitions);
        builder.setSplitKey(splitKey);
        builder.setWhere(where);
        builder.setFetchSize(fetchSize == 0 ? databaseInterface.getFetchSize() : fetchSize);
        builder.setQueryTimeOut(queryTimeOut == 0 ? databaseInterface.getQueryTimeout() : queryTimeOut);
        builder.setTestConfig(testConfig);
        builder.setLogConfig(logConfig);

        BaseRichInputFormat format =  builder.finish();
        return createInput(format, (databaseInterface.getDatabaseType() + DISTRIBUTED_TAG + "reader").toLowerCase());
    }

    protected DistributedJdbcInputFormatBuilder getBuilder(){
        throw new RuntimeException("子类必须覆盖getBuilder方法");
    }

    protected ArrayList<DataSource> buildConnections(){
        ArrayList<DataSource> sourceList = new ArrayList<>(connectionConfigs.size());
        for (ReaderConfig.ParameterConfig.ConnectionConfig connectionConfig : connectionConfigs) {
            String curUsername = (StringUtils.isBlank(connectionConfig.getUsername())) ? username : connectionConfig.getUsername();
            String curPassword = (StringUtils.isBlank(connectionConfig.getPassword())) ? password : connectionConfig.getPassword();
            String curJdbcUrl = DbUtil.formatJdbcUrl(connectionConfig.getJdbcUrl().get(0), null);
            for (String table : connectionConfig.getTable()) {
                DataSource source = new DataSource();
                source.setTable(table);
                source.setUserName(curUsername);
                source.setPassword(curPassword);
                source.setJdbcUrl(curJdbcUrl);

                sourceList.add(source);
            }
        }

        return sourceList;
    }

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        this.databaseInterface = databaseInterface;
    }
}
