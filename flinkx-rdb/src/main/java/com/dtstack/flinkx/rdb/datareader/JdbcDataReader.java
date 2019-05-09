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
import com.dtstack.flinkx.rdb.DatabaseInterface;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormatBuilder;
import com.dtstack.flinkx.inputformat.RichInputFormat;
import com.dtstack.flinkx.rdb.type.TypeConverterInterface;
import com.dtstack.flinkx.rdb.util.DBUtil;
import com.dtstack.flinkx.reader.DataReader;
import com.dtstack.flinkx.reader.MetaColumn;
import org.apache.commons.lang3.StringUtils;
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
public class JdbcDataReader extends DataReader {

    protected DatabaseInterface databaseInterface;

    protected TypeConverterInterface typeConverter;

    protected String dbUrl;

    protected String username;

    protected String password;

    protected List<MetaColumn> metaColumns;

    protected String table;

    protected String where;

    protected String splitKey;

    protected String increColumn;

    protected String startLocation;

    protected int fetchSize;

    protected int queryTimeOut;

    protected int requestAccumulatorInterval;

    protected boolean useMaxFunc;

    protected String customSql;

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
        dbUrl = DBUtil.formatJdbcUrl(readerConfig.getName(),dbUrl);
        username = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_USER_NAME);
        password = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_PASSWORD);
        table = readerConfig.getParameter().getConnection().get(0).getTable().get(0);
        where = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_WHERE);
        metaColumns = MetaColumn.getMetaColumns(readerConfig.getParameter().getColumn());
        fetchSize = readerConfig.getParameter().getIntVal(JdbcConfigKeys.KEY_FETCH_SIZE,0);
        queryTimeOut = readerConfig.getParameter().getIntVal(JdbcConfigKeys.KEY_QUERY_TIME_OUT,0);
        requestAccumulatorInterval = readerConfig.getParameter().getIntVal(JdbcConfigKeys.KEY_REQUEST_ACCUMULATOR_INTERVAL,2);
        splitKey = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_SPLIK_KEY);
        increColumn = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_INCRE_COLUMN);
        startLocation = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_START_LOCATION,null);
        customSql = readerConfig.getParameter().getStringVal(JdbcConfigKeys.KEY_CUSTOM_SQL,null);
        useMaxFunc = readerConfig.getParameter().getBooleanVal(JdbcConfigKeys.KEY_USE_MAX_FUNC,true);

        increColumn = StringUtils.isEmpty(increColumn) ? null : increColumn;
        if(StringUtils.isEmpty(increColumn)){
            useMaxFunc = false;
        }
    }

    @Override
    public DataStream<Row> readData() {
        // Read from JDBC
        JdbcInputFormatBuilder builder = new JdbcInputFormatBuilder();
        builder.setDrivername(databaseInterface.getDriverClass());
        builder.setDBUrl(dbUrl);
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
        builder.setRequestAccumulatorInterval(requestAccumulatorInterval);
        builder.setIncreCol(increColumn);
        builder.setIncreColType(getIncrementColType());
        builder.setStartLocation(startLocation);
        builder.setSplitKey(splitKey);
        builder.setNumPartitions(numPartitions);
        builder.setUseMaxFunc(useMaxFunc);
        builder.setCustomSql(customSql);
        builder.setHadoopConfig(hadoopConfig);


        boolean isSplitByKey = numPartitions > 1 && StringUtils.isNotEmpty(splitKey);

        String query;
        if (StringUtils.isNotEmpty(customSql)){
            query = DBUtil.buildQuerySqlWithCustomSql(databaseInterface, customSql, isSplitByKey, splitKey, StringUtils.isNotEmpty(increColumn));
        } else {
            query = DBUtil.getQuerySql(databaseInterface, table, metaColumns, splitKey, where, isSplitByKey, StringUtils.isNotEmpty(increColumn));
        }
        builder.setQuery(query);

        RichInputFormat format =  builder.finish();
        return createInput(format, (databaseInterface.getDatabaseType() + "reader").toLowerCase());
    }

    private String getIncrementColType(){
        if (StringUtils.isEmpty(increColumn)){
            return null;
        }

        for (MetaColumn metaColumn : metaColumns) {
            if(metaColumn.getName().equals(increColumn)){
                return metaColumn.getType();
            }
        }

        throw new IllegalArgumentException("There is no " + increColumn +" field in the columns");
    }
}
