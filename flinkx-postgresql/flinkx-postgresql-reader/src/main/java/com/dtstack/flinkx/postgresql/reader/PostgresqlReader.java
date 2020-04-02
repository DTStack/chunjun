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

package com.dtstack.flinkx.postgresql.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.inputformat.BaseRichInputFormat;
import com.dtstack.flinkx.postgresql.PostgresqlDatabaseMeta;
import com.dtstack.flinkx.postgresql.PostgresqlTypeConverter;
import com.dtstack.flinkx.postgresql.format.PostgresqlInputFormat;
import com.dtstack.flinkx.rdb.datareader.JdbcDataReader;
import com.dtstack.flinkx.rdb.datareader.QuerySqlBuilder;
import com.dtstack.flinkx.rdb.inputformat.JdbcInputFormatBuilder;
import com.dtstack.flinkx.rdb.util.DbUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * The reader plugin for PostgreSQL database
 *
 * @Company: www.dtstack.com
 * @author jiangbo
 */
public class PostgresqlReader extends JdbcDataReader {

    public PostgresqlReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        setDatabaseInterface(new PostgresqlDatabaseMeta());
        setTypeConverterInterface(new PostgresqlTypeConverter());
        dbUrl = DbUtil.formatJdbcUrl(dbUrl, null);
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        return new JdbcInputFormatBuilder(new PostgresqlInputFormat());
    }

    @Override
    public DataStream<Row> readData() {
        JdbcInputFormatBuilder builder = new JdbcInputFormatBuilder(new PostgresqlInputFormat());
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

        QuerySqlBuilder sqlBuilder = new PostgresqlQuerySqlBuilder(this);
        builder.setQuery(sqlBuilder.buildSql());

        BaseRichInputFormat format =  builder.finish();
        return createInput(format, (databaseInterface.getDatabaseType() + "reader").toLowerCase());
    }
}
