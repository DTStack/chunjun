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

package com.dtstack.chunjun.connector.sqlserver.sink;

import com.dtstack.chunjun.config.FieldConfig;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.jdbc.util.JdbcUtil;
import com.dtstack.chunjun.connector.sqlserver.dialect.SqlserverDialect;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class SqlserverOutputFormat extends JdbcOutputFormat {

    private final String findIdentityColumn =
            "select name from sys.columns where object_id=object_id(?) and is_identity = 'true'";

    @Override
    protected void openInternal(int taskNumber, int numTasks) {
        super.openInternal(taskNumber, numTasks);

        Statement statement = null;
        String sql =
                ((SqlserverDialect) jdbcDialect)
                        .getIdentityInsertOnSql(jdbcConfig.getSchema(), jdbcConfig.getTable());
        try {
            if (checkContainIdentityColumn()) {
                statement = dbConn.createStatement();
                statement.execute(sql);
            }
        } catch (SQLException e) {
            throw new ChunJunRuntimeException(e);
        } finally {
            JdbcUtil.closeDbResources(null, statement, null, false);
        }
    }

    private boolean checkContainIdentityColumn() {
        try {

            if (CollectionUtils.isNotEmpty(jdbcConfig.getColumn())) {
                // 为* 代表所有字段，返回true，不需要校验字段里是否有identity字段，后面执行的时候还会校验一次表是否还有identity column
                if (jdbcConfig.getColumn().get(0).getName().equals("*")) {
                    return true;
                }
                try (PreparedStatement preparedStatement =
                        dbConn.prepareStatement(findIdentityColumn)) {
                    preparedStatement.setString(
                            1,
                            jdbcDialect.buildTableInfoWithSchema(
                                    jdbcConfig.getSchema(), jdbcConfig.getTable()));
                    // 包含就为true，不包含就为false
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        Set<String> columnNames =
                                jdbcConfig.getColumn().stream()
                                        .map(FieldConfig::getName)
                                        .collect(Collectors.toSet());
                        while (resultSet.next()) {
                            if (columnNames.contains(resultSet.getString(1))) {
                                return true;
                            }
                        }
                    }
                }
                return false;
            }
            // 默认认为是包含的 因为后面执行具体sql的时候会判断表是否含有identity column
            return true;
        } catch (Exception e) {
            log.warn("select identity column failed..", e);
            return true;
        }
    }
}
