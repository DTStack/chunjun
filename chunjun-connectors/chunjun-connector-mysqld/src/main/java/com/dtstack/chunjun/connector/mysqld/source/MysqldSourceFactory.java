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
package com.dtstack.chunjun.connector.mysqld.source;

import com.dtstack.chunjun.config.SyncConf;
import com.dtstack.chunjun.connector.jdbc.config.ConnectionConf;
import com.dtstack.chunjun.connector.jdbc.config.DataSourceConf;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.distribute.DistributedJdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.source.distribute.DistributedJdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.distribute.DistributedJdbcSourceFactory;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.mysql.dialect.MysqlDialect;
import com.dtstack.chunjun.connector.mysqld.utils.MySqlDataSource;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MysqldSourceFactory extends DistributedJdbcSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(MysqldSourceFactory.class);

    private final AtomicInteger jdbcPatternCounter = new AtomicInteger(0);
    private final AtomicInteger jdbcQueryTableCounter = new AtomicInteger(0);

    // 默认是Mysql流式拉取
    private static final int DEFAULT_FETCH_SIZE = Integer.MIN_VALUE;

    public MysqldSourceFactory(SyncConf syncConf, StreamExecutionEnvironment env) {
        super(syncConf, env, new MysqlDialect());
        JdbcUtil.putExtParam(jdbcConf);
    }

    @Override
    protected int getDefaultFetchSize() {
        return DEFAULT_FETCH_SIZE;
    }

    @Override
    protected JdbcInputFormatBuilder getBuilder() {
        DistributedJdbcInputFormatBuilder builder =
                new DistributedJdbcInputFormatBuilder(new DistributedJdbcInputFormat());
        List<ConnectionConf> connectionConfList = jdbcConf.getConnection();
        List<DataSourceConf> dataSourceConfList = new ArrayList<>(connectionConfList.size());
        try {
            /** 可以是多个 connection、多个 database、多个 table， 或者 正则匹配的 database、table */
            for (ConnectionConf connectionConf : connectionConfList) {
                String currentUsername =
                        (StringUtils.isNotBlank(connectionConf.getUsername()))
                                ? connectionConf.getUsername()
                                : jdbcConf.getUsername();
                String currentPassword =
                        (StringUtils.isNotBlank(connectionConf.getPassword()))
                                ? connectionConf.getPassword()
                                : jdbcConf.getPassword();
                String jdbcUrl = connectionConf.obtainJdbcUrl();
                Connection connection =
                        MySqlDataSource.getDataSource(jdbcUrl, currentUsername, currentPassword)
                                .getConnection();
                for (String table : connectionConf.getTable()) {
                    dataSourceConfList.addAll(
                            allTables(
                                    connection,
                                    connectionConf.getSchema(),
                                    jdbcUrl,
                                    currentUsername,
                                    currentPassword,
                                    table));
                }
            }
        } catch (SQLException e) {
            throw new ChunJunRuntimeException(e);
        }
        builder.setSourceList(dataSourceConfList);
        return builder;
    }

    public List<DataSourceConf> allTables(
            Connection connection,
            String currSchema,
            String url,
            String user,
            String pass,
            String currTable)
            throws SQLException {
        Map<String, List<String>> tables = new HashMap<>();
        List<DataSourceConf> dataSourceConfList = new ArrayList<>();

        Pattern schema = Pattern.compile(currSchema);
        Pattern table = Pattern.compile(currTable);
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet catalogs = metaData.getCatalogs();
        while (catalogs.next()) {
            String schemaName = catalogs.getString(1);
            Matcher matcher = schema.matcher(schemaName);
            if (matcher.matches()) {
                tables.put(schemaName, new ArrayList<>());
            }
        }
        for (String databaseName : tables.keySet()) {
            ResultSet tableResult = metaData.getTables(databaseName, null, null, null);
            while (tableResult.next()) {
                String tableName = tableResult.getString("TABLE_NAME");
                LOG.info(
                        "query table [{}]: {}, table: {}.{}",
                        jdbcQueryTableCounter.incrementAndGet(),
                        url,
                        schema,
                        tableName);
                Matcher matcher = table.matcher(tableName);
                if (matcher.matches()) {
                    tables.get(databaseName).add(tableName);
                }
            }
        }

        for (String key : tables.keySet()) {
            for (String mTable : tables.get(key)) {
                DataSourceConf dataSourceConf = new DataSourceConf();
                dataSourceConf.setJdbcUrl(url);
                dataSourceConf.setUserName(user);
                dataSourceConf.setPassword(pass);
                dataSourceConf.setSchema(key);
                dataSourceConf.setTable(mTable);
                LOG.info(
                        "pattern jdbcurl: [{}] {}, table: {}.{}",
                        jdbcPatternCounter.incrementAndGet(),
                        url,
                        key,
                        mTable);
                dataSourceConfList.add(dataSourceConf);
            }
        }
        return dataSourceConfList;
    }
}
