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

package com.dtstack.flinkx.sqlserver.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.rdb.datareader.QuerySqlBuilder;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormatBuilder;
import com.dtstack.flinkx.sqlserver.SqlServerConfigKeys;
import com.dtstack.flinkx.sqlserver.SqlServerDatabaseMeta;
import com.dtstack.flinkx.sqlserver.format.SqlserverInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;


/**
 * SQLServer reader plugin
 * <p>
 * Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class SqlserverReader extends JdbcDataReader {
    //是否在sql语句后面添加 with(nolock) ,默认是false
    private Boolean withNoLock;

    public SqlserverReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        setDatabaseInterface(new SqlServerDatabaseMeta());
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        withNoLock = readerConfig.getParameter().getBooleanVal(SqlServerConfigKeys.WITH_NO_LOCK,false);
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        SqlserverInputFormat sqlserverInputFormat = new SqlserverInputFormat();
        return new JdbcInputFormatBuilder(sqlserverInputFormat);
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
        builder.setDataTransferConfig(dataTransferConfig);
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

        QuerySqlBuilder sqlBuilder = new SqlserverQuerySqlBuilder(this);
        builder.setQuery(sqlBuilder.buildSql());

        BaseRichInputFormat format =  builder.finish();
//        (databaseInterface.getDatabaseType() + "reader").toLowerCase()
        return createInput(format);
    }

    public Boolean getWithNoLock() {
        return withNoLock;
    }
}
