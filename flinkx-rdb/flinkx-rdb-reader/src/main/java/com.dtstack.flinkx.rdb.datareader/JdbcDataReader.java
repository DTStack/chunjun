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
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormatBuilder;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.reader.BaseDataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * The Reader plugin for any database that can be connected via JDBC.
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class JdbcDataReader extends BaseDataReader {

    protected DatabaseInterface databaseInterface;

    protected TypeConverterInterface typeConverter;

    protected String dbUrl;

    protected String username;

    protected String password;

    protected List<MetaColumn> metaColumns;

    protected String table;

    protected String where;

    protected String splitKey;

    protected int fetchSize;

    protected int queryTimeOut;

    protected String customSql;

    protected String orderByColumn;

    protected IncrementConfig incrementConfig;

    public void setDatabaseInterface(DatabaseInterface databaseInterface) {
        this.databaseInterface = databaseInterface;
    }

    public void setTypeConverterInterface(TypeConverterInterface typeConverter) {
        this.typeConverter = typeConverter;
    }

    public JdbcDataReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);

        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        dbUrl = readerConfig.getParameter().getConnection().get(0).getJdbcUrl().get(0);
        username = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_USER_NAME);
        password = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_PASSWORD);
        table = readerConfig.getParameter().getConnection().get(0).getTable().get(0);
        where = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_WHERE);
        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn());
        fetchSize = readerConfig.getParameter().getIntVal(JdbcConfigKeys.KEY_FETCH_SIZE,0);
        queryTimeOut = readerConfig.getParameter().getIntVal(JdbcConfigKeys.KEY_QUERY_TIME_OUT,0);
        splitKey = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_SPLIK_KEY);
        customSql = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_CUSTOM_SQL,null);
        orderByColumn = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_ORDER_BY_COLUMN,null);

        buildIncrementConfig(readerConfig);
    }

    @Override
    public DataStream<Row> readData() {
        JdbcInputFormatBuilder builder = getBuilder();
        builder.setDriverName(databaseInterface.getDriverClass());
        builder.setDbUrl(dbUrl);
        builder.setUsername(username);
        builder.setPassword(password);
        builder.setBytes(bytes);
        builder.setMonitorUrls(monitorUrls);
        builder.setTable(table);
        builder.setDatabaseInterface(databaseInterface);
        builder.setTypeConverter(typeConverter);
        builder.setMetaColumn(metaColumns);
        builder.setFetchSize(fetchSize == 0 ? databaseInterface.getFetchSize() : fetchSize);
        builder.setQueryTimeOut(queryTimeOut == 0 ? databaseInterface.getQueryTimeout() : queryTimeOut);
        builder.setIncrementConfig(incrementConfig);
        builder.setSplitKey(splitKey);
        builder.setNumPartitions(numPartitions);
        builder.setCustomSql(customSql);
        builder.setRestoreConfig(restoreConfig);
        builder.setHadoopConfig(hadoopConfig);
        builder.setTestConfig(testConfig);
        builder.setLogConfig(logConfig);

        QuerySqlBuilder sqlBuilder = new QuerySqlBuilder(this);
        builder.setQuery(sqlBuilder.buildSql());

        BaseRichInputFormat format =  builder.finish();
//        (databaseInterface.getDatabaseType() + "reader").toLowerCase()
        return createInput(format);
    }

    protected JdbcInputFormatBuilder getBuilder() {
        throw new RuntimeException("子类必须覆盖getBuilder方法");
    }

    private void buildIncrementConfig(ReaderConfig readerConfig){
        boolean polling = readerConfig.getParameter().getBooleanVal(JdbcConfigKeys.KEY_POLLING, false);
        Object incrementColumn = readerConfig.getParameter().getVal(JdbcConfigKeys.KEY_INCRE_COLUMN);
        String startLocation = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_START_LOCATION,null);
        boolean useMaxFunc = readerConfig.getParameter().getBooleanVal(JdbcConfigKeys.KEY_USE_MAX_FUNC, false);
        int requestAccumulatorInterval = readerConfig.getParameter().getIntVal(JdbcConfigKeys.KEY_REQUEST_ACCUMULATOR_INTERVAL, 2);
        long pollingInterval = readerConfig.getParameter().getLongVal(JdbcConfigKeys.KEY_POLLING_INTERVAL, 5000);

        incrementConfig = new IncrementConfig();
        if (incrementColumn != null && StringUtils.isNotEmpty(incrementColumn.toString())){
            String type = null;
            String name = null;
            int index = -1;

            String incrementColStr = String.valueOf(incrementColumn);
            if(NumberUtils.isNumber(incrementColStr)){
                MetaColumn metaColumn = metaColumns.get(Integer.parseInt(incrementColStr));
                type = metaColumn.getType();
                name = metaColumn.getName();
                index = metaColumn.getIndex();
            } else {
                for (MetaColumn metaColumn : metaColumns) {
                    if(metaColumn.getName().equals(incrementColStr)){
                        type = metaColumn.getType();
                        name = metaColumn.getName();
                        index = metaColumn.getIndex();
                        break;
                    }
                }
            }

            incrementConfig.setIncrement(true);
            incrementConfig.setPolling(polling);
            incrementConfig.setColumnName(name);
            incrementConfig.setColumnType(type);
            incrementConfig.setStartLocation(startLocation);
            incrementConfig.setUseMaxFunc(useMaxFunc);
            incrementConfig.setColumnIndex(index);
            incrementConfig.setRequestAccumulatorInterval(requestAccumulatorInterval);
            incrementConfig.setPollingInterval(pollingInterval);

            if (type == null || name == null){
                throw new IllegalArgumentException("There is no " + incrementColStr +" field in the columns");
            }
        }
    }

}
