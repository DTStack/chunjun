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
package com.dtstack.chunjun.connector.jdbc.source.distribute;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.config.TypeConfig;
import com.dtstack.chunjun.connector.jdbc.config.ConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.config.DataSourceConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.chunjun.util.ColumnBuildUtil;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

public abstract class DistributedJdbcSourceFactory extends JdbcSourceFactory {

    protected DistributedJdbcSourceFactory(
            SyncConfig syncConfig, StreamExecutionEnvironment env, JdbcDialect jdbcDialect) {
        super(syncConfig, env, jdbcDialect);
    }

    protected JdbcInputFormatBuilder getBuilder() {
        DistributedJdbcInputFormatBuilder builder =
                new DistributedJdbcInputFormatBuilder(new DistributedJdbcInputFormat());
        List<ConnectionConfig> connectionConfigList = jdbcConfig.getConnection();
        List<DataSourceConfig> dataSourceConfigList = new ArrayList<>(connectionConfigList.size());
        for (ConnectionConfig connectionConfig : connectionConfigList) {
            String currentUsername =
                    (StringUtils.isNotBlank(connectionConfig.getUsername()))
                            ? connectionConfig.getUsername()
                            : jdbcConfig.getUsername();
            String currentPassword =
                    (StringUtils.isNotBlank(connectionConfig.getPassword()))
                            ? connectionConfig.getPassword()
                            : jdbcConfig.getPassword();

            String schema = connectionConfig.getSchema();
            for (String table : connectionConfig.getTable()) {
                DataSourceConfig dataSourceConfig = new DataSourceConfig();
                dataSourceConfig.setUserName(currentUsername);
                dataSourceConfig.setPassword(currentPassword);
                dataSourceConfig.setJdbcUrl(connectionConfig.obtainJdbcUrl());
                dataSourceConfig.setTable(table);
                dataSourceConfig.setSchema(schema);

                dataSourceConfigList.add(dataSourceConfig);
            }
        }
        builder.setSourceList(dataSourceConfigList);
        return builder;
    }

    @Override
    protected void initColumnInfo() {
        columnNameList = new ArrayList<>();
        columnTypeList = new ArrayList<>();
        for (FieldConfig fieldConfig : jdbcConfig.getColumn()) {
            this.columnNameList.add(fieldConfig.getName());
            this.columnTypeList.add(fieldConfig.getType());
        }
        Pair<List<String>, List<TypeConfig>> columnPair =
                ColumnBuildUtil.handleColumnList(
                        jdbcConfig.getColumn(), this.columnNameList, this.columnTypeList);
        this.columnNameList = columnPair.getLeft();
        this.columnTypeList = columnPair.getRight();
    }
}
